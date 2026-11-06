#include <jni.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

extern int WexDownloaderRun(int argc, char **argv, const char *cwd);
extern void WexDownloaderCancel(void);

static JavaVM *g_vm;
static jobject g_stdout_cb;
static jobject g_stderr_cb;
static jmethodID g_on_line;
static pthread_mutex_t g_cb_lock = PTHREAD_MUTEX_INITIALIZER;

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    (void) reserved;
    g_vm = vm;
    return JNI_VERSION_1_6;
}

void WexDownloaderEmit(int is_err, const char *line) {
    if (!g_vm || !line) return;
    JNIEnv *env = NULL;
    jint rc = (*g_vm)->GetEnv(g_vm, (void **) &env, JNI_VERSION_1_6);
    int attached = 0;
    if (rc == JNI_EDETACHED) {
        if ((*g_vm)->AttachCurrentThread(g_vm, (void **) &env, NULL) != 0) return;
        attached = 1;
    } else if (rc != JNI_OK || env == NULL) {
        return;
    }
    pthread_mutex_lock(&g_cb_lock);
    jobject cb = is_err ? g_stderr_cb : g_stdout_cb;
    jmethodID mid = g_on_line;
    if (cb && mid) {
        jstring s = (*env)->NewStringUTF(env, line);
        if (s) {
            (*env)->CallVoidMethod(env, cb, mid, s);
            (*env)->ExceptionClear(env);
            (*env)->DeleteLocalRef(env, s);
        }
    }
    pthread_mutex_unlock(&g_cb_lock);
    if (attached) {
        (*g_vm)->DetachCurrentThread(g_vm);
    }
}

JNIEXPORT jint JNICALL
Java_client_wex_modules_WexDownloaderJni_nativeRun(
        JNIEnv *env, jclass cls,
        jobjectArray jargs, jstring jcwd,
        jobject stdoutCb, jobject stderrCb) {
    (void) cls;
    if (!env) return 2;

    jclass sink = (*env)->FindClass(env, "client/wex/modules/DownloaderLineSink");
    if (!sink) return 2;
    g_on_line = (*env)->GetMethodID(env, sink, "onLine", "(Ljava/lang/String;)V");
    (*env)->DeleteLocalRef(env, sink);
    if (!g_on_line) return 2;

    pthread_mutex_lock(&g_cb_lock);
    if (g_stdout_cb) { (*env)->DeleteGlobalRef(env, g_stdout_cb); g_stdout_cb = NULL; }
    if (g_stderr_cb) { (*env)->DeleteGlobalRef(env, g_stderr_cb); g_stderr_cb = NULL; }
    if (stdoutCb) g_stdout_cb = (*env)->NewGlobalRef(env, stdoutCb);
    if (stderrCb) g_stderr_cb = (*env)->NewGlobalRef(env, stderrCb);
    pthread_mutex_unlock(&g_cb_lock);

    int argc = jargs ? (*env)->GetArrayLength(env, jargs) : 0;
    char **argv = (char **) calloc((size_t) argc + 1, sizeof(char *));
    if (!argv) return 2;
    int i;
    for (i = 0; i < argc; i++) {
        jstring js = (jstring) (*env)->GetObjectArrayElement(env, jargs, i);
        if (!js) {
            argv[i] = strdup("");
            continue;
        }
        const char *utf = (*env)->GetStringUTFChars(env, js, NULL);
        argv[i] = strdup(utf ? utf : "");
        (*env)->ReleaseStringUTFChars(env, js, utf);
        (*env)->DeleteLocalRef(env, js);
    }

    const char *cwd = NULL;
    if (jcwd) cwd = (*env)->GetStringUTFChars(env, jcwd, NULL);

    int code = WexDownloaderRun(argc, argv, cwd);

    if (jcwd && cwd) (*env)->ReleaseStringUTFChars(env, jcwd, cwd);
    for (i = 0; i < argc; i++) free(argv[i]);
    free(argv);

    pthread_mutex_lock(&g_cb_lock);
    if (g_stdout_cb) { (*env)->DeleteGlobalRef(env, g_stdout_cb); g_stdout_cb = NULL; }
    if (g_stderr_cb) { (*env)->DeleteGlobalRef(env, g_stderr_cb); g_stderr_cb = NULL; }
    g_on_line = NULL;
    pthread_mutex_unlock(&g_cb_lock);
    return code;
}

JNIEXPORT void JNICALL
Java_client_wex_modules_WexDownloaderJni_nativeCancel(JNIEnv *env, jclass cls) {
    (void) env;
    (void) cls;
    WexDownloaderCancel();
}
