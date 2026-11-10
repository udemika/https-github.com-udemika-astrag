/*
 * astrag-core: native data plane for AstraG.
 *
 * Owns the public HTTP ports. Every request is relayed to the Python control
 * server (uvicorn on 127.0.0.1); when Python answers a media route with an
 * "X-Astrag-Core" directive, the response is served here from shared per-channel
 * buffers instead (HTTP MPEG-TS, HLS playlists and segments).
 *
 * Inputs: HTTP/HTTPS MPEG-TS and HLS (master/media playlists, AES-128), ordered
 * failover between inputs. HLS input is downloaded ahead (two connections) and
 * released at the stream bitrate like Cesbo Astra's sync buffer.
 *
 * Control: Python connects to an abstract unix socket and sends tab separated
 * commands (AUTH, ADD, DEL, KILL, LISTEN, UNLISTEN); the core reports STAT and
 * SESS+/SESS- lines.
 */
#define _GNU_SOURCE
#include <features.h>
/* Keep classic strtol/sscanf symbols: glibc 2.38 headers otherwise bind __isoc23_* and the binary
 * would not start on Ubuntu 22.04 (glibc 2.35). */
#undef __GLIBC_USE_C2X_STRTOL
#define __GLIBC_USE_C2X_STRTOL 0
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/epoll.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define TS 188
#define KF_N 48
#define PCR_HZ 27000000.0
#define PCR_WRAP (8589934592LL * 300LL)
#define MAX_URLS 16

static double g_now;
static int g_ep = -1;
static char g_secret[128];
static int g_upstream_port;
static int g_hls_parallel = 2; /* segments downloaded at once; 1 = sequential (variant 1) */
static SSL_CTX *g_ssl;
static unsigned long long g_sess_seq;

#define LOG(...) do { fprintf(stderr, "[astrag-core] " __VA_ARGS__); fputc('\n', stderr); } while (0)

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec * 1e-9;
}

static size_t min_sz(size_t a, size_t b) { return a < b ? a : b; }
static double clampd(double v, double lo, double hi) { return v < lo ? lo : (v > hi ? hi : v); }

/* ------------------------------------------------------------------ buffers */

typedef struct { uint8_t *p; size_t len, cap; } buf_t;

static int buf_reserve(buf_t *b, size_t n)
{
    if (b->len + n <= b->cap)
        return 0;
    size_t nc = b->cap ? b->cap : 1024;
    while (nc < b->len + n)
        nc *= 2;
    void *q = realloc(b->p, nc);
    if (!q)
        return -1;
    b->p = q;
    b->cap = nc;
    return 0;
}

static int buf_add(buf_t *b, const void *d, size_t n)
{
    if (!n)
        return 0;
    if (buf_reserve(b, n))
        return -1;
    memcpy(b->p + b->len, d, n);
    b->len += n;
    return 0;
}

static int buf_str(buf_t *b, const char *s) { return buf_add(b, s, strlen(s)); }

static int buf_fmt(buf_t *b, const char *fmt, ...)
{
    va_list ap;
    char tmp[1024];
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof tmp, fmt, ap);
    va_end(ap);
    if (n < 0)
        return -1;
    if ((size_t)n < sizeof tmp)
        return buf_add(b, tmp, n);
    if (buf_reserve(b, n + 1))
        return -1;
    va_start(ap, fmt);
    vsnprintf((char *)b->p + b->len, n + 1, fmt, ap);
    va_end(ap);
    b->len += n;
    return 0;
}

static void buf_drop(buf_t *b, size_t n)
{
    if (n >= b->len) {
        b->len = 0;
        return;
    }
    memmove(b->p, b->p + n, b->len - n);
    b->len -= n;
}

static void buf_free(buf_t *b)
{
    free(b->p);
    b->p = NULL;
    b->len = b->cap = 0;
}

/* --------------------------------------------------------------- event core */

enum { EV_LISTEN = 1, EV_FRONT, EV_UP, EV_FETCH, EV_CTL_LISTEN, EV_CTL, EV_DNS };

typedef struct ev {
    int type, fd, reg, dead;
    uint32_t mask;
    void *obj;
} ev_t;

static void ev_watch(ev_t *e, uint32_t mask)
{
    if (e->fd < 0 || e->dead)
        return;
    if (e->reg && e->mask == mask)
        return;
    struct epoll_event ee;
    memset(&ee, 0, sizeof ee);
    ee.events = mask;
    ee.data.ptr = e;
    if (epoll_ctl(g_ep, e->reg ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, e->fd, &ee) == 0) {
        e->reg = 1;
        e->mask = mask;
    }
}

static void ev_close(ev_t *e)
{
    if (e->fd >= 0) {
        if (e->reg)
            epoll_ctl(g_ep, EPOLL_CTL_DEL, e->fd, NULL);
        close(e->fd);
    }
    e->fd = -1;
    e->reg = 0;
}

typedef struct grave { void (*fn)(void *); void *p; struct grave *next; } grave_t;
static grave_t *g_grave;

/* Objects are freed only after the current epoll batch so stale events stay safe. */
static void bury(void (*fn)(void *), void *p)
{
    grave_t *g = malloc(sizeof *g);
    if (!g) {
        fn(p);
        return;
    }
    g->fn = fn;
    g->p = p;
    g->next = g_grave;
    g_grave = g;
}

static void reap(void)
{
    while (g_grave) {
        grave_t *g = g_grave;
        g_grave = g->next;
        g->fn(g->p);
        free(g);
    }
}

static void set_nonblock(int fd) { fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK); }

/* --------------------------------------------------------------------- DNS */

typedef void (*dns_cb)(void *arg, int ok, struct in_addr a);
typedef struct dnsq {
    char host[256];
    struct in_addr addr;
    int ok, cancelled;
    dns_cb cb;
    void *arg;
    struct dnsq *next;
} dnsq_t;

static pthread_mutex_t g_dmu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_dcv = PTHREAD_COND_INITIALIZER;
static dnsq_t *g_dqh, *g_dqt;
static int g_dpipe[2];
static ev_t g_dev;
static struct { char host[256]; struct in_addr a; double exp; } g_dcache[256];
static int g_dci;

static void *dns_thread(void *u)
{
    (void)u;
    for (;;) {
        pthread_mutex_lock(&g_dmu);
        while (!g_dqh)
            pthread_cond_wait(&g_dcv, &g_dmu);
        dnsq_t *q = g_dqh;
        g_dqh = q->next;
        if (!g_dqh)
            g_dqt = NULL;
        pthread_mutex_unlock(&g_dmu);
        struct addrinfo hints, *res = NULL;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        q->ok = getaddrinfo(q->host, NULL, &hints, &res) == 0 && res;
        if (q->ok)
            q->addr = ((struct sockaddr_in *)res->ai_addr)->sin_addr;
        if (res)
            freeaddrinfo(res);
        ssize_t w;
        do {
            w = write(g_dpipe[1], &q, sizeof q);
        } while (w < 0 && errno == EINTR);
    }
    return NULL;
}

/* Returns 1 with *out filled when resolved immediately, 0 when queued (*pend set). */
static int dns_lookup(const char *host, dns_cb cb, void *arg, struct in_addr *out, dnsq_t **pend)
{
    *pend = NULL;
    if (inet_pton(AF_INET, host, out) == 1)
        return 1;
    for (int i = 0; i < 256; i++)
        if (g_dcache[i].exp > g_now && !strcmp(g_dcache[i].host, host)) {
            *out = g_dcache[i].a;
            return 1;
        }
    dnsq_t *q = calloc(1, sizeof *q);
    if (!q)
        return -1;
    snprintf(q->host, sizeof q->host, "%s", host);
    q->cb = cb;
    q->arg = arg;
    pthread_mutex_lock(&g_dmu);
    if (g_dqt)
        g_dqt->next = q;
    else
        g_dqh = q;
    g_dqt = q;
    pthread_cond_signal(&g_dcv);
    pthread_mutex_unlock(&g_dmu);
    *pend = q;
    return 0;
}

static void dns_on_ready(void)
{
    dnsq_t *qs[64];
    ssize_t n = read(g_dpipe[0], qs, sizeof qs);
    if (n <= 0)
        return;
    for (ssize_t i = 0; i < n / (ssize_t)sizeof(dnsq_t *); i++) {
        dnsq_t *q = qs[i];
        if (q->ok) {
            snprintf(g_dcache[g_dci].host, sizeof g_dcache[0].host, "%s", q->host);
            g_dcache[g_dci].a = q->addr;
            g_dcache[g_dci].exp = g_now + 120.0;
            g_dci = (g_dci + 1) % 256;
        }
        if (!q->cancelled)
            q->cb(q->arg, q->ok, q->addr);
        free(q);
    }
}

/* --------------------------------------------------------------------- URLs */

static int b64(const char *in, size_t n, char *out, size_t outsz)
{
    if (outsz < 4 * ((n + 2) / 3) + 1)
        return -1;
    EVP_EncodeBlock((unsigned char *)out, (const unsigned char *)in, (int)n);
    return 0;
}

static int url_parse(const char *url, int *https, char *host, size_t hl, int *port, char *target, size_t tl,
                     char *auth, size_t al)
{
    const char *p = url;
    if (!strncasecmp(p, "http://", 7)) {
        *https = 0;
        p += 7;
    } else if (!strncasecmp(p, "https://", 8)) {
        *https = 1;
        p += 8;
    } else
        return -1;
    const char *end = p + strcspn(p, "/?#");
    auth[0] = 0;
    const char *at = memrchr(p, '@', end - p);
    if (at) {
        if (b64(p, at - p, auth, al))
            return -1;
        p = at + 1;
    }
    const char *colon = memchr(p, ':', end - p);
    const char *he = colon ? colon : end;
    if (he == p || (size_t)(he - p) >= hl)
        return -1;
    memcpy(host, p, he - p);
    host[he - p] = 0;
    *port = *https ? 443 : 80;
    if (colon) {
        int v = atoi(colon + 1);
        if (v <= 0 || v > 65535)
            return -1;
        *port = v;
    }
    size_t tlen = strcspn(end, "#");
    if (tlen == 0) {
        snprintf(target, tl, "/");
    } else if (*end == '?') {
        if (tlen + 2 > tl)
            return -1;
        target[0] = '/';
        memcpy(target + 1, end, tlen);
        target[tlen + 1] = 0;
    } else {
        if (tlen + 1 > tl)
            return -1;
        memcpy(target, end, tlen);
        target[tlen] = 0;
    }
    return 0;
}

/* Remove "." and ".." path segments in place (query string untouched). */
static void url_normalize(char *u)
{
    char *s = strstr(u, "://");
    if (!s)
        return;
    char *path = strchr(s + 3, '/');
    if (!path)
        return;
    char *q = path + strcspn(path, "?#");
    char tail[4096];
    snprintf(tail, sizeof tail, "%s", q);
    *q = 0;
    char out[4096];
    size_t o = 0;
    char *seg = path + 1;
    out[o++] = '/';
    for (;;) {
        char *sl = strchr(seg, '/');
        size_t n = sl ? (size_t)(sl - seg) : strlen(seg);
        if (n == 1 && seg[0] == '.') {
        } else if (n == 2 && seg[0] == '.' && seg[1] == '.') {
            if (o > 1) {
                o--;
                while (o > 0 && out[o - 1] != '/')
                    o--;
            }
        } else if (o + n + 1 < sizeof out) {
            memcpy(out + o, seg, n);
            o += n;
            if (sl)
                out[o++] = '/';
        }
        if (!sl)
            break;
        seg = sl + 1;
    }
    out[o] = 0;
    size_t room = 4096 - (path - u);
    snprintf(path, room, "%s%s", out, tail);
}

static void url_resolve(const char *base, const char *ref, char *out, size_t outsz)
{
    if (strstr(ref, "://") && strcspn(ref, ":") < strcspn(ref, "/?")) {
        snprintf(out, outsz, "%s", ref);
        return;
    }
    const char *s = strstr(base, "://");
    if (!s) {
        snprintf(out, outsz, "%s", ref);
        return;
    }
    if (ref[0] == '/' && ref[1] == '/') {
        snprintf(out, outsz, "%.*s:%s", (int)(s - base), base, ref);
        return;
    }
    const char *auth_end = s + 3 + strcspn(s + 3, "/?#");
    if (ref[0] == '/') {
        snprintf(out, outsz, "%.*s%s", (int)(auth_end - base), base, ref);
    } else if (ref[0] == '?') {
        size_t pl = strcspn(base, "?#");
        snprintf(out, outsz, "%.*s%s", (int)pl, base, ref);
    } else {
        size_t pl = strcspn(base, "?#");
        const char *last = NULL;
        for (const char *c = auth_end; c < base + pl; c++)
            if (*c == '/')
                last = c;
        if (!last)
            snprintf(out, outsz, "%.*s/%s", (int)(auth_end - base), base, ref);
        else
            snprintf(out, outsz, "%.*s%s", (int)(last - base + 1), base, ref);
    }
    if (outsz >= 4096)
        url_normalize(out);
}

/* -------------------------------------------------------------- HTTP client */

typedef struct fetch fetch_t;
typedef struct fetch_cb {
    void (*head)(fetch_t *);
    void (*data)(fetch_t *, const uint8_t *, size_t);
    void (*done)(fetch_t *, int ok, const char *err);
} fetch_cb_t;

enum { F_DNS, F_CONN, F_TLS, F_SEND, F_HEAD, F_BODY };

#define KA_MAX 48
typedef struct {
    int fd;
    SSL *ssl;
    int https;
    int port;
    char host[256];
    double expires;
} ka_slot_t;
static ka_slot_t g_ka[KA_MAX];

#define TLS_SESS_MAX 32
typedef struct {
    char host[256];
    int port;
    SSL_SESSION *sess;
} tls_sess_t;
static tls_sess_t g_tls_sess[TLS_SESS_MAX];
static int g_tls_sess_i;

static SSL_SESSION *tls_sess_get(const char *host, int port)
{
    for (int i = 0; i < TLS_SESS_MAX; i++)
        if (g_tls_sess[i].sess && g_tls_sess[i].port == port && !strcasecmp(g_tls_sess[i].host, host))
            return g_tls_sess[i].sess;
    return NULL;
}

static void tls_sess_put(const char *host, int port, SSL *ssl)
{
    if (!ssl || !host || !*host)
        return;
    SSL_SESSION *s = SSL_get1_session(ssl);
    if (!s)
        return;
    for (int i = 0; i < TLS_SESS_MAX; i++) {
        if (g_tls_sess[i].sess && g_tls_sess[i].port == port && !strcasecmp(g_tls_sess[i].host, host)) {
            SSL_SESSION_free(g_tls_sess[i].sess);
            g_tls_sess[i].sess = s;
            return;
        }
    }
    int idx = g_tls_sess_i;
    g_tls_sess_i = (g_tls_sess_i + 1) % TLS_SESS_MAX;
    if (g_tls_sess[idx].sess)
        SSL_SESSION_free(g_tls_sess[idx].sess);
    snprintf(g_tls_sess[idx].host, sizeof g_tls_sess[idx].host, "%s", host);
    g_tls_sess[idx].port = port;
    g_tls_sess[idx].sess = s;
}

static void ka_close_slot(ka_slot_t *s)
{
    if (s->ssl) {
        SSL_free(s->ssl);
        s->ssl = NULL;
    }
    if (s->fd >= 0) {
        close(s->fd);
        s->fd = -1;
    }
    s->host[0] = 0;
    s->expires = 0;
}

static void ka_init_once(void)
{
    static int inited = 0;
    if (!inited) {
        for (int i = 0; i < KA_MAX; i++)
            g_ka[i].fd = -1;
        inited = 1;
    }
}

static void ka_sweep(void)
{
    ka_init_once();
    for (int i = 0; i < KA_MAX; i++) {
        if (g_ka[i].fd >= 0 && g_now >= g_ka[i].expires)
            ka_close_slot(&g_ka[i]);
    }
}

static int ka_take(int https, const char *host, int port, int *fd_out, SSL **ssl_out)
{
    ka_init_once();
    for (int i = KA_MAX - 1; i >= 0; i--) {
        ka_slot_t *s = &g_ka[i];
        if (s->fd < 0)
            continue;
        if (g_now >= s->expires) {
            ka_close_slot(s);
            continue;
        }
        if (s->https == https && s->port == port && !strcasecmp(s->host, host)) {
            uint8_t b;
            ssize_t r = recv(s->fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
            if (r == 0 || (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                ka_close_slot(s);
                continue;
            }
            *fd_out = s->fd;
            *ssl_out = s->ssl;
            s->fd = -1;
            s->ssl = NULL;
            s->host[0] = 0;
            return 1;
        }
    }
    return 0;
}

static void ka_put(int https, const char *host, int port, int fd, SSL *ssl)
{
    ka_init_once();
    if (fd < 0)
        return;
    epoll_ctl(g_ep, EPOLL_CTL_DEL, fd, NULL);
    int victim = -1;
    double oldest = 1e30;
    for (int i = 0; i < KA_MAX; i++) {
        if (g_ka[i].fd < 0) {
            victim = i;
            break;
        }
        if (g_ka[i].expires < oldest) {
            oldest = g_ka[i].expires;
            victim = i;
        }
    }
    if (victim < 0)
        victim = 0;
    if (g_ka[victim].fd >= 0)
        ka_close_slot(&g_ka[victim]);
    g_ka[victim].fd = fd;
    g_ka[victim].ssl = ssl;
    g_ka[victim].https = https;
    g_ka[victim].port = port;
    snprintf(g_ka[victim].host, sizeof g_ka[victim].host, "%s", host);
    g_ka[victim].expires = g_now + 25.0;
}

struct fetch {
    ev_t ev;
    SSL *ssl;
    int st;
    char url[4096], host[256], target[4096], auth[512], ua[256];
    int port, https, redirects;
    double deadline, conn_to, idle_to;
    buf_t req;
    size_t req_off;
    buf_t head;
    int status, chunked, cst, cll;
    int conn_close, reused;
    int64_t clen, remain;
    char cl[32];
    char ctype[128];
    dnsq_t *dns;
    int paused;
    const char *derr;
    const fetch_cb_t *cb;
    void *owner;
    fetch_t *gnext, *gprev;
};

static fetch_t *g_fetches;

static void fetch_free(void *p)
{
    fetch_t *f = p;
    buf_free(&f->req);
    buf_free(&f->head);
    free(f);
}

static void fetch_kill(fetch_t *f)
{
    if (f->ev.dead)
        return;
    f->ev.dead = 1;
    if (f->dns) {
        f->dns->cancelled = 1;
        f->dns = NULL;
    }
    if (f->ssl) {
        SSL_free(f->ssl);
        f->ssl = NULL;
    }
    ev_close(&f->ev);
    if (f->gprev)
        f->gprev->gnext = f->gnext;
    else if (g_fetches == f)
        g_fetches = f->gnext;
    if (f->gnext)
        f->gnext->gprev = f->gprev;
    bury(fetch_free, f);
}

static void fetch_cancel(fetch_t *f)
{
    if (f)
        fetch_kill(f);
}

static void fetch_end(fetch_t *f, int ok, const char *err)
{
    if (f->ev.dead)
        return;
    const fetch_cb_t *cb = f->cb;
    fetch_kill(f);
    cb->done(f, ok, err);
}

static void fetch_begin(fetch_t *f);

/* Start-up errors are reported from the next sweep so fetch_start() never calls back synchronously. */
static void fetch_defer(fetch_t *f, const char *msg)
{
    f->derr = msg;
    f->deadline = 0;
}

static void fetch_connect(fetch_t *f, struct in_addr a)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        fetch_defer(f, "socket failed");
        return;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port = htons(f->port);
    sa.sin_addr = a;
    f->ev.fd = fd;
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) < 0 && errno != EINPROGRESS) {
        fetch_defer(f, "connect failed");
        return;
    }
    f->st = F_CONN;
    f->deadline = g_now + f->conn_to;
    ev_watch(&f->ev, EPOLLOUT);
}

static void fetch_dns_done(void *arg, int ok, struct in_addr a)
{
    fetch_t *f = arg;
    f->dns = NULL;
    if (f->ev.dead)
        return;
    if (!ok) {
        fetch_defer(f, "DNS lookup failed");
        return;
    }
    fetch_connect(f, a);
}

static int fetch_flush_req(fetch_t *f);

static void fetch_begin_ex(fetch_t *f, int allow_ka)
{
    if (url_parse(f->url, &f->https, f->host, sizeof f->host, &f->port, f->target, sizeof f->target, f->auth,
                  sizeof f->auth)) {
        fetch_defer(f, "invalid URL");
        return;
    }
    f->req.len = 0;
    f->req_off = 0;
    f->conn_close = 0;
    f->reused = 0;
    int is_php = strcasestr(f->target, ".php") != NULL;
    int defport = (f->https && f->port == 443) || (!f->https && f->port == 80);
    buf_fmt(&f->req, "GET %s HTTP/1.1\r\nHost: %s", f->target, f->host);
    if (!defport)
        buf_fmt(&f->req, ":%d", f->port);
    buf_fmt(&f->req,
            "\r\nUser-Agent: %s\r\nAccept: */*\r\nAccept-Encoding: identity\r\nCache-Control: no-cache\r\n"
            "Connection: %s\r\n",
            f->ua, is_php ? "close" : "keep-alive");
    if (f->auth[0])
        buf_fmt(&f->req, "Authorization: Basic %s\r\n", f->auth);
    buf_str(&f->req, "\r\n");
    if (allow_ka && !is_php) {
        int kfd = -1;
        SSL *kssl = NULL;
        if (ka_take(f->https, f->host, f->port, &kfd, &kssl)) {
            f->ev.fd = kfd;
            f->ev.reg = 0;
            f->ev.mask = 0;
            f->ssl = kssl;
            f->reused = 1;
            f->st = F_SEND;
            f->deadline = g_now + f->conn_to;
            fetch_flush_req(f);
            return;
        }
    }
    f->st = F_DNS;
    f->deadline = g_now + f->conn_to;
    struct in_addr a;
    dnsq_t *pend;
    int r = dns_lookup(f->host, fetch_dns_done, f, &a, &pend);
    if (r == 1)
        fetch_connect(f, a);
    else if (r == 0)
        f->dns = pend;
    else
        fetch_defer(f, "DNS queue failed");
}

static void fetch_begin(fetch_t *f)
{
    fetch_begin_ex(f, 1);
}

static fetch_t *fetch_start(const char *url, const char *ua, const fetch_cb_t *cb, void *owner, double conn_to,
                            double idle_to)
{
    fetch_t *f = calloc(1, sizeof *f);
    if (!f)
        return NULL;
    f->ev.type = EV_FETCH;
    f->ev.fd = -1;
    f->ev.obj = f;
    snprintf(f->url, sizeof f->url, "%s", url);
    snprintf(f->ua, sizeof f->ua, "%s", ua && *ua ? ua : "AstraG/2.0");
    f->cb = cb;
    f->owner = owner;
    f->conn_to = conn_to;
    f->idle_to = idle_to;
    f->clen = -1;
    f->gnext = g_fetches;
    if (g_fetches)
        g_fetches->gprev = f;
    g_fetches = f;
    fetch_begin(f);
    return f;
}

static void fetch_pause(fetch_t *f, int on)
{
    if (!f || f->ev.dead || f->paused == on)
        return;
    f->paused = on;
    if (f->st >= F_HEAD)
        ev_watch(&f->ev, on ? 0 : EPOLLIN);
}

static void fetch_restart_redirect(fetch_t *f, const char *loc)
{
    char nu[4096];
    url_resolve(f->url, loc, nu, sizeof nu);
    snprintf(f->url, sizeof f->url, "%s", nu);
    if (f->ssl) {
        SSL_free(f->ssl);
        f->ssl = NULL;
    }
    ev_close(&f->ev);
    f->head.len = 0;
    f->status = 0;
    f->chunked = 0;
    f->clen = -1;
    f->ctype[0] = 0;
    f->redirects++;
    fetch_begin(f);
}

static void fetch_recycle_ka(fetch_t *f)
{
    if (!f->ev.dead && f->ev.fd >= 0 && !f->conn_close && f->status >= 200 && f->status < 400 &&
        !strcasestr(f->target, ".php")) {
        ka_put(f->https, f->host, f->port, f->ev.fd, f->ssl);
        f->ev.fd = -1;
        f->ev.reg = 0;
            f->ev.mask = 0;
        f->ssl = NULL;
    }
}

static void fetch_body(fetch_t *f, const uint8_t *d, size_t n)
{
    if (f->chunked) {
        while (n > 0 && !f->ev.dead) {
            if (f->cst == 0 || f->cst == 3) {
                uint8_t c = *d++;
                n--;
                if (c == '\n') {
                    f->cl[f->cll] = 0;
                    if (f->cst == 3) {
                        if (f->cll == 0) {
                            if (n == 0)
                                fetch_recycle_ka(f);
                            fetch_end(f, 1, NULL);
                            return;
                        }
                        f->cll = 0;
                        continue;
                    }
                    f->remain = strtoll(f->cl, NULL, 16);
                    f->cll = 0;
                    f->cst = f->remain > 0 ? 1 : 3;
                } else if (c != '\r' && f->cll < (int)sizeof f->cl - 1) {
                    if (f->cst == 0 && c == ';')
                        f->cl[f->cll++] = 0;
                    else
                        f->cl[f->cll++] = c;
                }
            } else if (f->cst == 1) {
                size_t t = min_sz(n, (size_t)f->remain);
                f->cb->data(f, d, t);
                d += t;
                n -= t;
                f->remain -= t;
                if (f->remain == 0)
                    f->cst = 2;
            } else {
                uint8_t c = *d++;
                n--;
                if (c == '\n')
                    f->cst = 0;
            }
        }
        return;
    }
    if (f->clen >= 0) {
        size_t t = min_sz(n, (size_t)f->remain);
        if (t)
            f->cb->data(f, d, t);
        if (f->ev.dead)
            return;
        f->remain -= t;
        if (f->remain <= 0) {
            if (n == t)
                fetch_recycle_ka(f);
            fetch_end(f, 1, NULL);
        }
        return;
    }
    f->cb->data(f, d, n);
}

static void fetch_parse_head(fetch_t *f, size_t hl)
{
    char *h = (char *)f->head.p;
    h[hl - 2] = 0;
    f->status = 0;
    sscanf(h, "HTTP/%*d.%*d %d", &f->status);
    if (!f->status)
        sscanf(h, "HTTP/%*d %d", &f->status);
    char loc[4096] = "";
    char *line = strchr(h, '\n');
    while (line && *++line) {
        char *e = strchr(line, '\n');
        if (e)
            *e = 0;
        size_t ll = strlen(line);
        if (ll && line[ll - 1] == '\r')
            line[--ll] = 0;
        char *colon = strchr(line, ':');
        if (colon) {
            *colon = 0;
            char *v = colon + 1;
            while (*v == ' ' || *v == '\t')
                v++;
            if (!strcasecmp(line, "content-length"))
                f->clen = strtoll(v, NULL, 10);
            else if (!strcasecmp(line, "transfer-encoding") && strcasestr(v, "chunked"))
                f->chunked = 1;
            else if (!strcasecmp(line, "connection") && strcasestr(v, "close"))
                f->conn_close = 1;
            else if (!strcasecmp(line, "location"))
                snprintf(loc, sizeof loc, "%s", v);
            else if (!strcasecmp(line, "content-type"))
                snprintf(f->ctype, sizeof f->ctype, "%s", v);
        }
        line = e;
    }
    if (f->chunked)
        f->clen = -1;
    f->remain = f->clen;
    if ((f->status == 301 || f->status == 302 || f->status == 303 || f->status == 307 || f->status == 308) &&
        loc[0] && f->redirects < 8) {
        if (f->clen == 0 && (f->head.len - hl) == 0)
            fetch_recycle_ka(f);
        fetch_restart_redirect(f, loc);
        return;
    }
    f->st = F_BODY;
    f->cb->head(f);
    if (f->ev.dead)
        return;
    if (f->status == 204 || f->status == 304 || f->clen == 0) {
        if ((f->head.len - hl) == 0)
            fetch_recycle_ka(f);
        fetch_end(f, 1, NULL);
        return;
    }
    size_t left = f->head.len - hl;
    if (left)
        fetch_body(f, f->head.p + hl, left);
}

static void fetch_bytes(fetch_t *f, const uint8_t *d, size_t n)
{
    if (f->st == F_HEAD) {
        buf_add(&f->head, d, n);
        uint8_t *e = memmem(f->head.p, f->head.len, "\r\n\r\n", 4);
        if (!e) {
            if (f->head.len > 65536)
                fetch_end(f, 0, "response header too large");
            return;
        }
        fetch_parse_head(f, e - f->head.p + 4);
        return;
    }
    fetch_body(f, d, n);
}

static void fetch_retry_stale(fetch_t *f)
{
    if (f->ssl) {
        SSL_free(f->ssl);
        f->ssl = NULL;
    }
    ev_close(&f->ev);
    f->head.len = 0;
    f->status = 0;
    f->chunked = 0;
    f->clen = -1;
    f->ctype[0] = 0;
    f->reused = 0;
    fetch_begin_ex(f, 0);
}

static void fetch_eof(fetch_t *f, int err)
{
    if (f->st == F_HEAD || f->st == F_SEND) {
        if (f->reused) {
            fetch_retry_stale(f);
            return;
        }
        fetch_end(f, 0, err ? "connection error" : "connection closed before response");
        return;
    }
    if (f->chunked || (f->clen >= 0 && f->remain > 0)) {
        fetch_end(f, 0, "body truncated");
        return;
    }
    fetch_end(f, !err, err ? "connection error" : NULL);
}

static ssize_t fetch_recv(fetch_t *f, uint8_t *b, size_t n)
{
    if (f->ssl) {
        int r = SSL_read(f->ssl, b, (int)n);
        if (r > 0)
            return r;
        int e = SSL_get_error(f->ssl, r);
        if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
            errno = EAGAIN;
            return -1;
        }
        if (e == SSL_ERROR_ZERO_RETURN)
            return 0;
        if (e == SSL_ERROR_SYSCALL && ERR_peek_error() == 0 && errno == 0)
            return 0;
        ERR_clear_error();
        errno = EIO;
        return -1;
    }
    return recv(f->ev.fd, b, n, 0);
}

static void fetch_read(fetch_t *f, int force)
{
    static uint8_t rb[65536];
    int loops = 0;
    while (!f->ev.dead && (!f->paused || force)) {
        errno = 0;
        ssize_t n = fetch_recv(f, rb, sizeof rb);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                if (force && !f->ssl)
                    fetch_eof(f, 1);
                return;
            }
            fetch_eof(f, 1);
            return;
        }
        if (n == 0) {
            fetch_eof(f, 0);
            return;
        }
        f->deadline = g_now + f->idle_to;
        fetch_bytes(f, rb, n);
        if (!f->ssl && ++loops >= 4)
            return;
        if (force)
            force = 0;
    }
}

static int fetch_flush_req(fetch_t *f)
{
    while (f->req_off < f->req.len) {
        ssize_t n;
        if (f->ssl) {
            int r = SSL_write(f->ssl, f->req.p + f->req_off, (int)(f->req.len - f->req_off));
            if (r <= 0) {
                int e = SSL_get_error(f->ssl, r);
                if (e == SSL_ERROR_WANT_WRITE) {
                    ev_watch(&f->ev, EPOLLOUT);
                    return 0;
                }
                if (e == SSL_ERROR_WANT_READ) {
                    ev_watch(&f->ev, EPOLLIN);
                    return 0;
                }
                if (f->reused) {
                    fetch_retry_stale(f);
                    return 0;
                }
                fetch_end(f, 0, "TLS write failed");
                return -1;
            }
            n = r;
        } else {
            n = send(f->ev.fd, f->req.p + f->req_off, f->req.len - f->req_off, MSG_NOSIGNAL);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    ev_watch(&f->ev, EPOLLOUT);
                    return 0;
                }
                if (f->reused) {
                    fetch_retry_stale(f);
                    return 0;
                }
                fetch_end(f, 0, "send failed");
                return -1;
            }
        }
        f->req_off += n;
    }
    f->st = F_HEAD;
    f->deadline = g_now + f->idle_to;
    ev_watch(&f->ev, f->paused ? 0 : EPOLLIN);
    return 1;
}

static void fetch_on_event(fetch_t *f, uint32_t evs)
{
    if (f->st == F_CONN) {
        int err = 0;
        socklen_t l = sizeof err;
        getsockopt(f->ev.fd, SOL_SOCKET, SO_ERROR, &err, &l);
        if (err) {
            char m[96];
            snprintf(m, sizeof m, "connect failed: %s", strerror(err));
            fetch_end(f, 0, m);
            return;
        }
        if (f->https) {
            f->ssl = SSL_new(g_ssl);
            if (!f->ssl) {
                fetch_end(f, 0, "TLS init failed");
                return;
            }
            SSL_set_fd(f->ssl, f->ev.fd);
            struct in_addr tmp;
            if (inet_pton(AF_INET, f->host, &tmp) != 1)
                SSL_set_tlsext_host_name(f->ssl, f->host);
            SSL_SESSION *csess = tls_sess_get(f->host, f->port);
            if (csess)
                SSL_set_session(f->ssl, csess);
            SSL_set_connect_state(f->ssl);
            f->st = F_TLS;
        } else {
            f->st = F_SEND;
        }
    }
    if (f->st == F_TLS) {
        ERR_clear_error();
        int r = SSL_do_handshake(f->ssl);
        if (r != 1) {
            int e = SSL_get_error(f->ssl, r);
            if (e == SSL_ERROR_WANT_READ) {
                ev_watch(&f->ev, EPOLLIN);
                return;
            }
            if (e == SSL_ERROR_WANT_WRITE) {
                ev_watch(&f->ev, EPOLLOUT);
                return;
            }
            fetch_end(f, 0, "TLS handshake failed");
            return;
        }
        tls_sess_put(f->host, f->port, f->ssl);
        f->st = F_SEND;
    }
    if (f->st == F_SEND) {
        if (fetch_flush_req(f) <= 0)
            return;
    }
    if (f->st == F_HEAD || f->st == F_BODY)
        fetch_read(f, (evs & (EPOLLERR | EPOLLHUP)) && f->paused);
}

static void fetch_sweep(void)
{
    ka_sweep();
    for (fetch_t *f = g_fetches, *nx; f; f = nx) {
        nx = f->gnext;
        if (f->ev.dead)
            continue;
        if (f->derr) {
            fetch_end(f, 0, f->derr);
            continue;
        }
        if (f->paused) {
            f->deadline = g_now + f->idle_to;
            continue;
        }
        if (f->ssl && f->st >= F_HEAD && SSL_pending(f->ssl) > 0) {
            fetch_read(f, 0);
            continue;
        }
        if (g_now > f->deadline)
            fetch_end(f, 0, f->st < F_HEAD ? "connect timeout" : "read timeout");
    }
}

/* -------------------------------------------------------------- MPEG-TS core */

static uint32_t g_crc_tab[256];

static void crc_init(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i << 24;
        for (int k = 0; k < 8; k++)
            c = (c & 0x80000000u) ? (c << 1) ^ 0x04C11DB7u : (c << 1);
        g_crc_tab[i] = c;
    }
}

static uint32_t crc32_mpeg(const uint8_t *d, size_t n)
{
    uint32_t c = 0xFFFFFFFFu;
    while (n--)
        c = (c << 8) ^ g_crc_tab[((c >> 24) ^ *d++) & 0xFF];
    return c;
}

enum { K_NONE = 0, K_VIDEO = 1, K_AUDIO = 2, K_PMT = 3, K_OTHER = 4 };

typedef struct psicap {
    int pid;
    uint8_t sec[1024];
    int have, need;
    uint8_t pk[4][TS];
    int npk;
    uint8_t done[4][TS];
    int ndone;
} psicap_t;

typedef struct oseg {
    int ref;
    long long seq;
    double dur;
    int disc;
    uint8_t *data;
    size_t len;
} oseg_t;

static void oseg_unref(oseg_t *s)
{
    if (s && --s->ref == 0) {
        free(s->data);
        free(s);
    }
}

typedef struct front front_t;
typedef struct hls hls_t;

enum { IN_WAIT, IN_TS, IN_HLS };

typedef struct channel {
    char id[128], name[160];
    char *urls[MAX_URLS];
    int nurls, cur, fails_row;
    double seg_target;
    int seg_keep;
    struct channel *next;
    /* input */
    int mode;
    double retry_at, in_started, last_data, last_good;
    fetch_t *tsf;
    int ts_checked, probe_m3u;
    buf_t probe;
    hls_t *hls;
    char last_error[256];
    unsigned long long input_errors;
    char ua[256];
    int drop_sdt, drop_eit;
    /* TS state */
    uint8_t carry[TS];
    int carry_n, synced, vid_scan_rem;
    int8_t in_cc[8192], out_cc[8192], hls_cc[8192];
    uint8_t kind[8192], stype[8192];
    psicap_t caps[9];
    int ncaps;
    int pmt_pid[8], pnr[8], npmt, tsid;
    int pcr_pid;
    int64_t cur_pcr, br_pcr0;
    uint64_t br_pos0;
    double pcr_bitrate;
    double gop; /* keyframe interval, seconds (decaying max) */
    /* ring */
    uint8_t *ring;
    size_t rcap;
    uint64_t wpos;
    uint64_t kf_pos[KF_N];
    int64_t kf_pcr[KF_N];
    double kf_wall[KF_N];
    int kf_head, kf_cnt;
    double next_resize;
    /* TS viewers / HLS waiters */
    front_t *clients;
    int nclients;
    front_t *waiters;
    double last_flush;
    /* HLS out */
    int hls_on, cur_started, cur_has_kf, cur_disc, disc_pending;
    double hls_last_req, cur_t0_wall;
    int64_t cur_t0_pcr;
    oseg_t *segs[16];
    int nsegs;
    long long next_seq;
    buf_t cseg;
    /* stats */
    uint64_t win_bytes, packets, cc_err, pes_err, sync_losses;
    double win_start, bitrate;
    int scr_win, scrambled;
} channel_t;

static channel_t *g_channels;
static buf_t g_scr;

static channel_t *ch_find(const char *id)
{
    for (channel_t *c = g_channels; c; c = c->next)
        if (!strcmp(c->id, id))
            return c;
    return NULL;
}

static void ts_reset_psi(channel_t *ch)
{
    memset(ch->kind, 0, sizeof ch->kind);
    memset(ch->stype, 0, sizeof ch->stype);
    memset(ch->in_cc, -1, sizeof ch->in_cc);
    ch->ncaps = 0;
    ch->npmt = 0;
    ch->pcr_pid = -1;
    ch->cur_pcr = -1;
    ch->br_pcr0 = -1;
    ch->kf_cnt = 0;
    ch->synced = 0;
    ch->carry_n = 0;
    ch->vid_scan_rem = 0;
}

static psicap_t *cap_for(channel_t *ch, int pid, int create)
{
    for (int i = 0; i < ch->ncaps; i++)
        if (ch->caps[i].pid == pid)
            return &ch->caps[i];
    if (!create || ch->ncaps >= 9)
        return NULL;
    psicap_t *c = &ch->caps[ch->ncaps++];
    memset(c, 0, sizeof *c);
    c->pid = pid;
    return c;
}

static int is_video_stype(int st)
{
    return st == 0x01 || st == 0x02 || st == 0x10 || st == 0x1B || st == 0x24 || st == 0x42 || st == 0xD1 ||
           st == 0xEA || st == 0x33;
}

static int is_audio_stype(int st)
{
    return st == 0x03 || st == 0x04 || st == 0x0F || st == 0x11 || st == 0x1C || st == 0x2D || st == 0x81 ||
           st == 0x82 || st == 0x83 || st == 0x84 || st == 0x87;
}

static void parse_pat(channel_t *ch, const uint8_t *s, int sl)
{
    int end = 3 + sl - 4;
    int npids[8], npnr[8], n = 0;
    for (int i = 8; i + 4 <= end && n < 8; i += 4) {
        int pnr = (s[i] << 8) | s[i + 1];
        int pid = ((s[i + 2] & 0x1F) << 8) | s[i + 3];
        if (pnr == 0)
            continue;
        npnr[n] = pnr;
        npids[n++] = pid;
    }
    for (int i = 0; i < ch->npmt; i++) {
        int keep = 0;
        for (int k = 0; k < n; k++)
            keep |= npids[k] == ch->pmt_pid[i];
        if (!keep && ch->kind[ch->pmt_pid[i]] == K_PMT)
            ch->kind[ch->pmt_pid[i]] = K_NONE;
    }
    ch->tsid = (s[3] << 8) | s[4];
    ch->npmt = n;
    for (int i = 0; i < n; i++) {
        ch->pmt_pid[i] = npids[i];
        ch->pnr[i] = npnr[i];
        ch->kind[npids[i]] = K_PMT;
    }
}

static void parse_pmt(channel_t *ch, const uint8_t *s, int sl)
{
    int end = 3 + sl - 4;
    if (end < 12)
        return;
    int pcr = ((s[8] & 0x1F) << 8) | s[9];
    int pil = ((s[10] & 0x0F) << 8) | s[11];
    if (pcr != 0x1FFF)
        ch->pcr_pid = pcr;
    for (int i = 12 + pil; i + 5 <= end;) {
        int st = s[i];
        int pid = ((s[i + 1] & 0x1F) << 8) | s[i + 2];
        int el = ((s[i + 3] & 0x0F) << 8) | s[i + 4];
        int kind = K_OTHER;
        if (is_video_stype(st))
            kind = K_VIDEO;
        else if (is_audio_stype(st))
            kind = K_AUDIO;
        else if (st == 0x06) {
            for (int d = i + 5; d + 2 <= i + 5 + el && d + 2 <= end; d += 2 + s[d + 1]) {
                int tag = s[d];
                if (tag == 0x6A || tag == 0x7A || tag == 0x7C || tag == 0x7F)
                    kind = K_AUDIO;
                if (tag == 0x05 && d + 6 <= end && (!memcmp(s + d + 2, "AC-3", 4) || !memcmp(s + d + 2, "EAC3", 4)))
                    kind = K_AUDIO;
            }
        }
        if (pid < 8191 && ch->kind[pid] != K_PMT) {
            ch->kind[pid] = kind;
            ch->stype[pid] = st;
        }
        i += 5 + el;
    }
}

static void psi_push(channel_t *ch, int pid, const uint8_t *p, int off, int pusi)
{
    psicap_t *c = cap_for(ch, pid, 1);
    if (!c || off >= TS)
        return;
    if (pusi) {
        int s = off + 1 + p[off];
        c->need = 0;
        if (s + 3 > TS)
            return;
        int sl = ((p[s + 1] & 0x0F) << 8) | p[s + 2];
        if (3 + sl > (int)sizeof c->sec || sl < 9)
            return;
        c->need = 3 + sl;
        int take = TS - s < c->need ? TS - s : c->need;
        memcpy(c->sec, p + s, take);
        c->have = take;
        c->npk = 0;
        memcpy(c->pk[c->npk++], p, TS);
    } else if (c->need && c->have < c->need && c->npk < 4) {
        int take = TS - off < c->need - c->have ? TS - off : c->need - c->have;
        memcpy(c->sec + c->have, p + off, take);
        c->have += take;
        memcpy(c->pk[c->npk++], p, TS);
    } else {
        return;
    }
    if (c->need && c->have >= c->need) {
        int sl = c->need - 3;
        if (crc32_mpeg(c->sec, c->need) == 0) {
            if (pid == 0 && c->sec[0] == 0x00)
                parse_pat(ch, c->sec, sl);
            else if (c->sec[0] == 0x02)
                parse_pmt(ch, c->sec, sl);
            memcpy(c->done, c->pk, sizeof c->pk);
            c->ndone = c->npk;
        }
        c->need = 0;
    }
}

/* Returns 1 on keyframe NAL, -1 on non-IDR slice NAL, 0 if only AUD/SEI headers seen so far. */
static int scan_nal_kf(const uint8_t *p, int start, int st)
{
    for (int i = start; i + 4 <= TS; i++) {
        if (p[i] || p[i + 1] || p[i + 2] != 1)
            continue;
        uint8_t b = p[i + 3];
        if (st == 0x24) {
            int t = (b >> 1) & 0x3F;
            if ((t >= 16 && t <= 21) || t == 32 || t == 33)
                return 1;
            if (t <= 9)
                return -1;
        } else if (st == 0x01 || st == 0x02) {
            if (b == 0xB3)
                return 1;
            if (b == 0x00)
                return -1;
        } else {
            int t = b & 0x1F;
            if (t == 5 || t == 7)
                return 1;
            if (t == 1)
                return -1;
        }
        i += 2;
    }
    return 0;
}

/* Returns 1 to keep the packet. Normalizes CC and strips upstream discontinuity flags for continuous TS output. */
static int ts_packet(channel_t *ch, uint8_t *p, int hls, int *is_kf)
{
    (void)hls;
    int pid = ((p[1] & 0x1F) << 8) | p[2];
    if ((pid == 0x11 && ch->drop_sdt) || (pid == 0x12 && ch->drop_eit))
        return 0;
    ch->packets++;
    if (pid == 0x1FFF)
        return 1;
    int tei = p[1] & 0x80, pusi = p[1] & 0x40, scr = p[3] & 0xC0;
    int afc = (p[3] >> 4) & 3, cc = p[3] & 15;
    int off = 4, rai = 0, disc = 0;
    if (afc & 2) {
        int al = p[4];
        off = 5 + al;
        if (al > 0) {
            int fl = p[5];
            disc = fl & 0x80;
            rai = fl & 0x40;
            if ((fl & 0x10) && al >= 7) {
                int64_t base = ((int64_t)p[6] << 25) | (p[7] << 17) | (p[8] << 9) | (p[9] << 1) | (p[10] >> 7);
                int64_t pcr = base * 300 + (((p[10] & 1) << 8) | p[11]);
                if (ch->pcr_pid < 0)
                    ch->pcr_pid = pid;
                if (pid == ch->pcr_pid)
                    ch->cur_pcr = pcr;
            }
            if (disc) {
                p[5] &= 0x7F;
                disc = 0;
            }
        }
        if (off > TS)
            off = TS;
    }
    if (tei) {
        ch->in_cc[pid] = -1;
        return 1;
    }
    if (afc & 1) {
        if (ch->in_cc[pid] >= 0 && !disc && cc != ((ch->in_cc[pid] + 1) & 15) && cc != ch->in_cc[pid])
            ch->cc_err++;
        ch->in_cc[pid] = cc;
        int o = ch->out_cc[pid] < 0 ? cc : ((ch->out_cc[pid] + 1) & 15);
        ch->out_cc[pid] = o;
        p[3] = (p[3] & 0xF0) | o;
    } else if (ch->out_cc[pid] >= 0) {
        p[3] = (p[3] & 0xF0) | ch->out_cc[pid];
    }
    if (scr)
        ch->scr_win = 1;
    int kind = ch->kind[pid];
    if (!scr && (afc & 1) && (pid == 0 || kind == K_PMT))
        psi_push(ch, pid, p, off, pusi);
    if (pusi && !scr && (afc & 1) && off + 4 <= TS) {
        int startcode = p[off] == 0 && p[off + 1] == 0 && p[off + 2] == 1;
        if (kind == K_VIDEO || kind == K_AUDIO) {
            if (!startcode)
                ch->pes_err++;
        } else if (kind == K_NONE && startcode && pid > 0x1F && ch->npmt == 0) {
            int sid = p[off + 3];
            if (sid >= 0xE0 && sid <= 0xEF)
                ch->kind[pid] = kind = K_VIDEO;
            else if ((sid >= 0xC0 && sid <= 0xDF) || sid == 0xBD)
                ch->kind[pid] = kind = K_AUDIO;
        }
    }
    if (kind == K_VIDEO && !scr && (afc & 1)) {
        if (rai) {
            *is_kf = 1;
            ch->vid_scan_rem = 0;
        } else if (pusi) {
            if (off + 9 <= TS && p[off] == 0 && p[off + 1] == 0 && p[off + 2] == 1) {
                int r = scan_nal_kf(p, off + 9 + p[off + 8], ch->stype[pid]);
                if (r == 1) {
                    *is_kf = 1;
                    ch->vid_scan_rem = 0;
                } else if (r == 0) {
                    ch->vid_scan_rem = 5;
                } else {
                    ch->vid_scan_rem = 0;
                }
            } else {
                ch->vid_scan_rem = 0;
            }
        } else if (ch->vid_scan_rem > 0 && off + 4 <= TS) {
            int r = scan_nal_kf(p, off, ch->stype[pid]);
            if (r == 1) {
                *is_kf = 1;
                ch->vid_scan_rem = 0;
            } else if (r == -1) {
                ch->vid_scan_rem = 0;
            } else {
                ch->vid_scan_rem--;
            }
        }
    }
    return 1;
}

/* ------------------------------------------------------------ channel ring */

static void ring_resize(channel_t *ch, size_t ncap)
{
    ncap = ncap / TS * TS;
    if (ncap < TS * 64 || ncap == ch->rcap)
        return;
    uint8_t *nr = malloc(ncap);
    if (!nr)
        return;
    uint64_t keep = min_sz(ch->wpos, min_sz(ch->rcap, ncap));
    keep -= keep % TS;
    for (uint64_t p = ch->wpos - keep; p < ch->wpos;) {
        size_t so = p % ch->rcap, dso = p % ncap;
        size_t n = min_sz(min_sz(ch->rcap - so, ncap - dso), ch->wpos - p);
        memcpy(nr + dso, ch->ring + so, n);
        p += n;
    }
    free(ch->ring);
    ch->ring = nr;
    ch->rcap = ncap;
}

static uint64_t ring_low(channel_t *ch)
{
    uint64_t room = ch->rcap - 64 * TS;
    return ch->wpos > room ? ch->wpos - room : 0;
}

static void ring_copy(channel_t *ch, buf_t *dst, uint64_t from, uint64_t to)
{
    while (from < to) {
        size_t o = from % ch->rcap;
        size_t n = min_sz(ch->rcap - o, to - from);
        buf_add(dst, ch->ring + o, n);
        from += n;
    }
}

static void prefix_build(channel_t *ch, buf_t *b)
{
    psicap_t *pat = cap_for(ch, 0, 0);
    if (!pat || !pat->ndone)
        return;
    buf_add(b, pat->done, pat->ndone * TS);
    for (int i = 0; i < ch->npmt; i++) {
        psicap_t *c = cap_for(ch, ch->pmt_pid[i], 0);
        if (c && c->ndone)
            buf_add(b, c->done, c->ndone * TS);
    }
}

/* ------------------------------------------------------------- HLS output */

static double seg_elapsed(channel_t *ch, int64_t pcr)
{
    if (pcr >= 0 && ch->cur_t0_pcr >= 0) {
        double d = ((pcr - ch->cur_t0_pcr + PCR_WRAP) % PCR_WRAP) / PCR_HZ;
        if (d >= 0 && d < 60)
            return d;
    }
    return g_now - ch->cur_t0_wall;
}

static void hls_seg_append(channel_t *ch, const uint8_t *src, size_t len)
{
    if (!len)
        return;
    size_t base = ch->cseg.len;
    buf_add(&ch->cseg, src, len);
    uint8_t *dst = ch->cseg.p + base;
    for (size_t i = 0; i + TS <= len; i += TS) {
        uint8_t *p = dst + i;
        int pid = ((p[1] & 0x1F) << 8) | p[2];
        if (pid == 0x1FFF)
            continue;
        int afc = (p[3] >> 4) & 3;
        if (afc & 1) {
            int cc = p[3] & 15;
            int o = ch->hls_cc[pid] < 0 ? cc : ((ch->hls_cc[pid] + 1) & 15);
            ch->hls_cc[pid] = o;
            p[3] = (p[3] & 0xF0) | o;
        } else if (ch->hls_cc[pid] >= 0) {
            p[3] = (p[3] & 0xF0) | ch->hls_cc[pid];
        }
    }
}

static void seg_begin_ex(channel_t *ch, int64_t pcr, double wall, int has_kf)
{
    ch->cseg.len = 0;
    buf_t pre = {0};
    prefix_build(ch, &pre);
    if (pre.len)
        hls_seg_append(ch, pre.p, pre.len);
    buf_free(&pre);
    ch->cur_t0_pcr = pcr;
    ch->cur_t0_wall = wall;
    ch->cur_started = 1;
    ch->cur_has_kf = has_kf;
    ch->cur_disc = ch->disc_pending;
    ch->disc_pending = 0;
}

static void seg_finish(channel_t *ch, double dur)
{
    if (ch->cseg.len < TS * 8)
        return;
    oseg_t *s = calloc(1, sizeof *s);
    if (!s)
        return;
    s->ref = 1;
    s->seq = ch->next_seq++;
    s->dur = dur > 0.05 ? dur : ch->seg_target;
    s->disc = ch->cur_disc;
    s->data = ch->cseg.p;
    s->len = ch->cseg.len;
    ch->cseg.p = NULL;
    ch->cseg.len = ch->cseg.cap = 0;
    /* Keep at least 3 segments (RFC 8216 Section 6.2.2) so new HLS viewers never start with only 2 segments. */
    int min_keep = ch->seg_keep < 3 ? 3 : ch->seg_keep;
    int keep = g_now - ch->hls_last_req > 60.0 ? 3 : min_keep;
    while (ch->nsegs > 0 && (ch->nsegs == 16 || ch->nsegs >= keep)) {
        oseg_unref(ch->segs[0]);
        memmove(ch->segs, ch->segs + 1, (ch->nsegs - 1) * sizeof ch->segs[0]);
        ch->nsegs--;
    }
    ch->segs[ch->nsegs++] = s;
}

static void hls_out_off(channel_t *ch)
{
    for (int i = 0; i < ch->nsegs; i++)
        oseg_unref(ch->segs[i]);
    ch->nsegs = 0;
    buf_free(&ch->cseg);
    ch->cur_started = 0;
    ch->cur_has_kf = 0;
    ch->hls_on = 0;
}

static void hls_out_feed(channel_t *ch, const uint8_t *s, size_t o, const size_t *kfo, const int64_t *kfp, int nk)
{
    if (!ch->hls_on)
        return;
    size_t run = 0;
    for (int k = 0; k < nk; k++) {
        if (!ch->cur_started) {
            run = kfo[k];
            seg_begin_ex(ch, kfp[k], g_now, 1);
            continue;
        }
        double d = seg_elapsed(ch, kfp[k]);
        if (d >= ch->seg_target * 0.85 || (!ch->cur_has_kf && d >= 0.4)) {
            hls_seg_append(ch, s + run, kfo[k] - run);
            seg_finish(ch, d);
            run = kfo[k];
            seg_begin_ex(ch, kfp[k], g_now, 1);
        }
    }
    if (!ch->cur_started) {
        if (ch->kf_cnt == 0 && ch->gop <= 0)
            seg_begin_ex(ch, ch->cur_pcr, g_now, 0);
        else
            return;
    }
    hls_seg_append(ch, s + run, o - run);
    double d = seg_elapsed(ch, ch->cur_pcr);
    /* Never chop 10.4s-12s 4K GOPs mid-frame at 9.0s: wait for the real IDR keyframe up to max(16s, gop*1.35). */
    double max_unaligned_dur = (ch->kf_cnt > 0 || ch->gop > 0)
                                   ? fmax(ch->seg_target * 4.5, fmax(16.0, ch->gop * 1.35))
                                   : (ch->seg_target * 2.5);
    if (d >= max_unaligned_dur || ch->cseg.len > 48u * 1024 * 1024) {
        seg_finish(ch, d);
        seg_begin_ex(ch, ch->cur_pcr, g_now, 0);
    }
}

/* First HLS request: build segments from the ring so the playlist is usable immediately. */
static void hls_out_activate(channel_t *ch)
{
    ch->hls_on = 1;
    ch->cur_started = 0;
    ch->cur_has_kf = 0;
    ch->cseg.len = 0;
    uint64_t lo = ring_low(ch);
    int newest = -1, oldest = -1;
    for (int i = 0; i < ch->kf_cnt; i++) {
        int idx = (ch->kf_head - 1 - i + KF_N) % KF_N;
        if (ch->kf_pos[idx] < lo || ch->kf_pos[idx] >= ch->wpos)
            break;
        if (newest < 0)
            newest = idx;
        oldest = idx;
    }
    if (newest < 0)
        return;
    if (oldest != newest) {
        seg_begin_ex(ch, ch->kf_pcr[oldest], ch->kf_wall[oldest], 1);
        buf_t tmp = {0};
        ring_copy(ch, &tmp, ch->kf_pos[oldest], ch->kf_pos[newest]);
        if (tmp.len)
            hls_seg_append(ch, tmp.p, tmp.len);
        buf_free(&tmp);
        double d = -1;
        if (ch->kf_pcr[oldest] >= 0 && ch->kf_pcr[newest] >= 0)
            d = ((ch->kf_pcr[newest] - ch->kf_pcr[oldest] + PCR_WRAP) % PCR_WRAP) / PCR_HZ;
        if (d <= 0 || d > 60)
            d = ch->kf_wall[newest] - ch->kf_wall[oldest];
        if (d >= 0.3)
            seg_finish(ch, d);
    }
    seg_begin_ex(ch, ch->kf_pcr[newest], ch->kf_wall[newest], 1);
    buf_t tmp2 = {0};
    ring_copy(ch, &tmp2, ch->kf_pos[newest], ch->wpos);
    if (tmp2.len)
        hls_seg_append(ch, tmp2.p, tmp2.len);
    buf_free(&tmp2);
}

/* --------------------------------------------------------------- TS feed */

static void ts_feed(channel_t *ch, const uint8_t *d, size_t n, int hls)
{
    if (!n)
        return;
    ch->last_data = g_now;
    ch->win_bytes += n;
    g_scr.len = 0;
    buf_add(&g_scr, ch->carry, ch->carry_n);
    buf_add(&g_scr, d, n);
    ch->carry_n = 0;
    uint8_t *s = g_scr.p;
    size_t len = g_scr.len, i = 0, o = 0;
    size_t kfo[64];
    int64_t kfp[64];
    int nk = 0;
    size_t last_v_pusi_o = 0;
    int have_v_pusi = 0;
    while (i + TS <= len) {
        if (s[i] != 0x47 || (!ch->synced && i + 2 * TS <= len && s[i + TS] != 0x47)) {
            if (ch->synced)
                ch->sync_losses++;
            ch->synced = 0;
            i++;
            continue;
        }
        ch->synced = 1;
        if (o != i)
            memmove(s + o, s + i, TS);
        int pid = ((s[o + 1] & 0x1F) << 8) | s[o + 2];
        if ((s[o + 1] & 0x40) && ch->kind[pid] == K_VIDEO) {
            last_v_pusi_o = o;
            have_v_pusi = 1;
        }
        int kf = 0;
        if (ts_packet(ch, s + o, hls, &kf)) {
            if (kf && nk < 64) {
                size_t kf_off = ( !(s[o + 1] & 0x40) && have_v_pusi ) ? last_v_pusi_o : o;
                if (nk == 0 || kf_off > kfo[nk - 1]) {
                    kfo[nk] = kf_off;
                    kfp[nk++] = ch->cur_pcr;
                }
            }
            o += TS;
        }
        i += TS;
    }
    size_t rem = len - i;
    if (rem >= TS)
        rem = TS - 1;
    memcpy(ch->carry, s + len - rem, rem);
    ch->carry_n = (int)rem;
    if (!o)
        return;
    uint64_t base = ch->wpos;
    size_t w = o > ch->rcap ? ch->rcap : o;
    const uint8_t *src = s + (o - w);
    uint64_t p = base + (o - w);
    while (w) {
        size_t ro = p % ch->rcap;
        size_t m = min_sz(w, ch->rcap - ro);
        memcpy(ch->ring + ro, src, m);
        src += m;
        p += m;
        w -= m;
    }
    for (int k = 0; k < nk; k++) {
        if (ch->kf_cnt) {
            int prev = (ch->kf_head - 1 + KF_N) % KF_N;
            double iv = -1;
            if (kfp[k] >= 0 && ch->kf_pcr[prev] >= 0)
                iv = ((kfp[k] - ch->kf_pcr[prev] + PCR_WRAP) % PCR_WRAP) / PCR_HZ;
            if (iv < 0 || iv > 30)
                iv = g_now - ch->kf_wall[prev];
            if (iv > 0.2 && iv <= 30)
                ch->gop = iv > ch->gop ? iv : 0.95 * ch->gop + 0.05 * iv;
        }
        ch->kf_pos[ch->kf_head] = base + kfo[k];
        ch->kf_pcr[ch->kf_head] = kfp[k];
        ch->kf_wall[ch->kf_head] = g_now;
        ch->kf_head = (ch->kf_head + 1) % KF_N;
        if (ch->kf_cnt < KF_N)
            ch->kf_cnt++;
    }
    ch->wpos = base + o;
    if (ch->pcr_pid >= 0 && ch->cur_pcr >= 0) {
        if (ch->br_pcr0 < 0) {
            ch->br_pcr0 = ch->cur_pcr;
            ch->br_pos0 = ch->wpos;
        } else {
            double dt = ((ch->cur_pcr - ch->br_pcr0 + PCR_WRAP) % PCR_WRAP) / PCR_HZ;
            if (dt > 30 || dt < 0) {
                ch->br_pcr0 = -1;
            } else if (dt >= (ch->pcr_bitrate > 0 ? 1.5 : 0.5)) {
                double inst_br = (ch->wpos - ch->br_pos0) * 8.0 / dt;
                ch->pcr_bitrate = ch->pcr_bitrate > 0 ? (0.7 * ch->pcr_bitrate + 0.3 * inst_br) : inst_br;
                ch->br_pcr0 = ch->cur_pcr;
                ch->br_pos0 = ch->wpos;
            }
        }
    }
    hls_out_feed(ch, s, o, kfo, kfp, nk);
}

/* ------------------------------------------------------------ control out */

typedef struct ctl {
    ev_t ev;
    buf_t in, out;
    int authed;
} ctl_t;
static ctl_t *g_ctl;
static ev_t g_ctl_lev;

static void ctl_flush(void);

static void ctl_send(const char *fmt, ...)
{
    if (!g_ctl || !g_ctl->authed)
        return;
    if (g_ctl->out.len > 4u * 1024 * 1024)
        return;
    char tmp[4096];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof tmp, fmt, ap);
    va_end(ap);
    if (n <= 0)
        return;
    if (n >= (int)sizeof tmp) {
        n = sizeof tmp - 1;
        tmp[n - 1] = '\n';
    }
    buf_add(&g_ctl->out, tmp, n);
    ctl_flush();
}

/* Strip characters that would break the tab separated control protocol. */
static void clean_field(char *s)
{
    for (; *s; s++)
        if (*s == '\t' || *s == '\n' || *s == '\r')
            *s = ' ';
}

/* --------------------------------------------------------- front (HTTP) */

enum { FR_HEAD, FR_PROXY, FR_TS, FR_RESP, FR_WAIT };

typedef struct up up_t;

struct front {
    ev_t ev;
    int mode;
    char ip[64];
    buf_t in, out;
    size_t out_off;
    oseg_t *seg;
    size_t seg_off;
    up_t *up;
    int ws, cl_eof, close_when_done, blocked;
    double deadline;
    char ua[200], login[96];
    channel_t *ch;
    uint64_t pos;
    buf_t pre;
    size_t pre_off;
    double last_progress;
    unsigned long long sess;
    char hls_pre[700], hls_suf[700];
    int head_only;
    double wait_until;
    front_t *cnext, *cprev;
    front_t *gnext, *gprev;
};

struct up {
    ev_t ev;
    front_t *fr;
    int connecting, eof;
    buf_t send;
    size_t send_off;
    buf_t hb;
    int head_done, paused;
};

static front_t *g_fronts;

static void up_close(up_t *u);
static void front_flush(front_t *fr);

static void front_free(void *p)
{
    front_t *fr = p;
    buf_free(&fr->in);
    buf_free(&fr->out);
    buf_free(&fr->pre);
    free(fr);
}

static void ch_list_remove(front_t **head, front_t *fr)
{
    if (fr->cprev)
        fr->cprev->cnext = fr->cnext;
    else if (*head == fr)
        *head = fr->cnext;
    if (fr->cnext)
        fr->cnext->cprev = fr->cprev;
    fr->cnext = fr->cprev = NULL;
}

static void front_close(front_t *fr)
{
    if (fr->ev.dead)
        return;
    fr->ev.dead = 1;
    if (fr->up)
        up_close(fr->up);
    if (fr->mode == FR_TS && fr->ch) {
        ch_list_remove(&fr->ch->clients, fr);
        fr->ch->nclients--;
        ctl_send("SESS-\t%llu\n", fr->sess);
    } else if (fr->mode == FR_WAIT && fr->ch) {
        ch_list_remove(&fr->ch->waiters, fr);
    }
    fr->ch = NULL;
    if (fr->seg) {
        oseg_unref(fr->seg);
        fr->seg = NULL;
    }
    ev_close(&fr->ev);
    if (fr->gprev)
        fr->gprev->gnext = fr->gnext;
    else if (g_fronts == fr)
        g_fronts = fr->gnext;
    if (fr->gnext)
        fr->gnext->gprev = fr->gprev;
    bury(front_free, fr);
}

static void up_free(void *p)
{
    up_t *u = p;
    buf_free(&u->send);
    buf_free(&u->hb);
    free(u);
}

static void up_close(up_t *u)
{
    if (u->ev.dead)
        return;
    u->ev.dead = 1;
    ev_close(&u->ev);
    if (u->fr)
        u->fr->up = NULL;
    u->fr = NULL;
    bury(up_free, u);
}

static void front_respond(front_t *fr, int code, const char *reason, const char *ctype, const char *extra,
                          const void *body, size_t blen, oseg_t *seg, int head_only)
{
    if (fr->up)
        up_close(fr->up);
    fr->mode = FR_RESP;
    fr->out.len = 0;
    fr->out_off = 0;
    size_t total = seg ? seg->len : blen;
    buf_fmt(&fr->out,
            "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\nAccess-Control-Allow-Origin: *\r\n%s"
            "Connection: close\r\n\r\n",
            code, reason, ctype, total, extra ? extra : "");
    if (!head_only) {
        if (seg) {
            seg->ref++;
            fr->seg = seg;
            fr->seg_off = 0;
        } else {
            buf_add(&fr->out, body, blen);
        }
    }
    front_flush(fr);
}

static void front_error(front_t *fr, int code, const char *reason)
{
    char body[128];
    int n = snprintf(body, sizeof body, "{\"detail\":\"%s\"}", reason);
    front_respond(fr, code, reason, "application/json", code == 503 ? "Retry-After: 1\r\n" : NULL, body, n, NULL,
                  0);
}

/* Send pending proxy/fixed response bytes. */
static void front_flush(front_t *fr)
{
    if (fr->ev.dead)
        return;
    for (;;) {
        struct iovec iov[2];
        int n = 0;
        if (fr->out_off < fr->out.len) {
            iov[n].iov_base = fr->out.p + fr->out_off;
            iov[n++].iov_len = fr->out.len - fr->out_off;
        }
        if (fr->seg && fr->seg_off < fr->seg->len) {
            iov[n].iov_base = fr->seg->data + fr->seg_off;
            iov[n++].iov_len = fr->seg->len - fr->seg_off;
        }
        if (!n)
            break;
        ssize_t w = writev(fr->ev.fd, iov, n);
        if (w < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                ev_watch(&fr->ev, EPOLLOUT | (fr->mode == FR_PROXY && !fr->cl_eof ? EPOLLIN : 0));
                return;
            }
            front_close(fr);
            return;
        }
        size_t a = min_sz((size_t)w, fr->out.len - fr->out_off);
        fr->out_off += a;
        w -= a;
        if (w && fr->seg)
            fr->seg_off += w;
        fr->deadline = g_now + 60;
    }
    fr->out.len = 0;
    fr->out_off = 0;
    if (fr->mode == FR_RESP) {
        front_close(fr);
        return;
    }
    if (fr->mode == FR_PROXY) {
        if (!fr->up) {
            front_close(fr);
            return;
        }
        if (fr->up->paused) {
            fr->up->paused = 0;
            ev_watch(&fr->up->ev, EPOLLIN | (fr->up->send_off < fr->up->send.len ? EPOLLOUT : 0));
        }
        ev_watch(&fr->ev, fr->cl_eof ? 0 : EPOLLIN);
    }
}

/* ---- TS viewers */

/* Media seconds between keyframe slot idx and the live edge (PCR when sane, wall clock otherwise). */
static double kf_age(channel_t *ch, int idx)
{
    if (ch->kf_pcr[idx] >= 0 && ch->cur_pcr >= 0) {
        double d = ((ch->cur_pcr - ch->kf_pcr[idx] + PCR_WRAP) % PCR_WRAP) / PCR_HZ;
        if (d >= 0 && d < 60)
            return d;
    }
    return g_now - ch->kf_wall[idx];
}

/* New viewers start at a keyframe ~START_BURST_S behind live so the player's buffer begins full
 * (Python engine sent the last whole segment; 1 s of burst underran VLC's default 1 s cache). */
#define START_BURST_S 3.0
#define MAX_BURST_BYTES (8u * 1024 * 1024)

static uint64_t pick_start(channel_t *ch)
{
    uint64_t lo = ring_low(ch);
    if (ch->wpos == 0)
        return 0;
    int64_t cand = -1;
    for (int i = 0; i < ch->kf_cnt; i++) {
        int idx = (ch->kf_head - 1 - i + KF_N) % KF_N;
        uint64_t p = ch->kf_pos[idx];
        if (p < lo || p >= ch->wpos)
            break;
        uint64_t dist = ch->wpos - p;
        double age = kf_age(ch, idx);
        /* Avoid jumping back 20+ MB on 10.4s 4K GOPs if the previous keyframe already gives >= 1.2s / 1.5 MB */
        if (cand >= 0 && (dist > MAX_BURST_BYTES || age > 8.0) && (ch->wpos - (uint64_t)cand) >= 384 * 1024)
            return (uint64_t)cand;
        cand = (int64_t)p;
        if (age >= START_BURST_S || dist >= (4u * 1024 * 1024))
            return p;
    }
    if (cand >= 0)
        return (uint64_t)cand;
    uint64_t back = min_sz(ch->wpos - lo, 512 * 1024);
    back -= back % TS;
    return ch->wpos - back;
}

/* Prefix PSI packets get CC values that continue into the stream at `start`. */
static void client_prefix(channel_t *ch, buf_t *pre, uint64_t start)
{
    size_t base = pre->len;
    prefix_build(ch, pre);
    size_t np = (pre->len - base) / TS;
    if (!np)
        return;
    int pids[16], ccs[16], found[16], npid = 0;
    for (size_t k = 0; k < np && npid < 16; k++) {
        uint8_t *p = pre->p + base + k * TS;
        int pid = ((p[1] & 0x1F) << 8) | p[2], dup = 0;
        for (int j = 0; j < npid; j++)
            dup |= pids[j] == pid;
        if (!dup) {
            pids[npid] = pid;
            found[npid++] = 0;
        }
    }
    int left = npid;
    for (uint64_t p = start; p + TS <= ch->wpos && p < start + 4u * 1024 * 1024 && left; p += TS) {
        const uint8_t *q = ch->ring + (p % ch->rcap);
        int pid = ((q[1] & 0x1F) << 8) | q[2];
        for (int j = 0; j < npid; j++)
            if (!found[j] && pids[j] == pid && (q[3] & 0x10)) {
                found[j] = 1;
                ccs[j] = q[3] & 15;
                left--;
            }
    }
    for (int j = 0; j < npid; j++) {
        if (!found[j])
            continue;
        int c = ccs[j];
        for (size_t k = np; k-- > 0;) {
            uint8_t *p = pre->p + base + k * TS;
            if ((((p[1] & 0x1F) << 8) | p[2]) == pids[j]) {
                c = (c - 1) & 15;
                p[3] = (p[3] & 0xF0) | c;
            }
        }
    }
}

static void client_send(front_t *c)
{
    channel_t *ch = c->ch;
    if (!ch || c->ev.dead)
        return;
    for (int it = 0; it < 8; it++) {
        uint64_t lo = ring_low(ch);
        if (c->pos < lo) {
            c->pos = pick_start(ch);
            c->pre.len = 0;
            c->pre_off = 0;
            client_prefix(ch, &c->pre, c->pos);
        }
        struct iovec iov[3];
        int n = 0;
        size_t prelen = c->pre.len - c->pre_off;
        if (prelen) {
            iov[n].iov_base = c->pre.p + c->pre_off;
            iov[n++].iov_len = prelen;
        }
        uint64_t avail = ch->wpos - c->pos;
        if (avail > 4u * 1024 * 1024)
            avail = 4u * 1024 * 1024;
        if (avail) {
            size_t a = c->pos % ch->rcap;
            size_t first = min_sz(avail, ch->rcap - a);
            iov[n].iov_base = ch->ring + a;
            iov[n++].iov_len = first;
            if (avail > first) {
                iov[n].iov_base = ch->ring;
                iov[n++].iov_len = avail - first;
            }
        }
        if (!n) {
            c->blocked = 0;
            ev_watch(&c->ev, EPOLLIN);
            return;
        }
        ssize_t w = writev(c->ev.fd, iov, n);
        if (w < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                c->blocked = 1;
                ev_watch(&c->ev, EPOLLIN | EPOLLOUT);
                return;
            }
            front_close(c);
            return;
        }
        c->last_progress = g_now;
        size_t a = min_sz((size_t)w, prelen);
        c->pre_off += a;
        if (c->pre_off >= c->pre.len) {
            c->pre.len = 0;
            c->pre_off = 0;
        }
        c->pos += (size_t)w - a;
        if ((size_t)w < prelen + avail) {
            c->blocked = 1;
            ev_watch(&c->ev, EPOLLIN | EPOLLOUT);
            return;
        }
    }
}

static void start_ts_client(front_t *fr, channel_t *ch, const char *login)
{
    if (fr->up)
        up_close(fr->up);
    fr->mode = FR_TS;
    fr->ch = ch;
    snprintf(fr->login, sizeof fr->login, "%s", login ? login : "");
    clean_field(fr->login);
    fr->pre.len = 0;
    fr->pre_off = 0;
    buf_str(&fr->pre, "HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\nCache-Control: no-store, no-cache, "
                      "must-revalidate\r\nPragma: no-cache\r\nAccept-Ranges: none\r\nAccess-Control-Allow-Origin: "
                      "*\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n");
    fr->pos = pick_start(ch);
    client_prefix(ch, &fr->pre, fr->pos);
    fr->cnext = ch->clients;
    if (ch->clients)
        ch->clients->cprev = fr;
    ch->clients = fr;
    ch->nclients++;
    fr->sess = ++g_sess_seq;
    fr->last_progress = g_now;
    ctl_send("SESS+\t%llu\t%s\t%s\t%s\t%s\n", fr->sess, ch->id, fr->ip, fr->login, fr->ua);
    client_send(fr);
}

/* ---- HLS output responses */

static void send_playlist(front_t *fr, channel_t *ch)
{
    buf_t b = {0};
    double maxd = ch->seg_target;
    for (int i = 0; i < ch->nsegs; i++)
        if (ch->segs[i]->dur > maxd)
            maxd = ch->segs[i]->dur;
    buf_fmt(&b, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:%lld\n",
            (int)ceil(maxd), ch->nsegs ? ch->segs[0]->seq : 0LL);
    for (int i = 0; i < ch->nsegs; i++) {
        oseg_t *s = ch->segs[i];
        if (s->disc && i > 0)
            buf_str(&b, "#EXT-X-DISCONTINUITY\n");
        buf_fmt(&b, "#EXTINF:%.3f,\n%ssegment-%lld.ts%s\n", s->dur, fr->hls_pre, s->seq, fr->hls_suf);
    }
    front_respond(fr, 200, "OK", "application/vnd.apple.mpegurl",
                  "Cache-Control: no-store, no-cache, must-revalidate\r\n", b.p, b.len, NULL, fr->head_only);
    buf_free(&b);
}

static void wait_remove(front_t *fr)
{
    if (fr->mode == FR_WAIT && fr->ch) {
        ch_list_remove(&fr->ch->waiters, fr);
        fr->ch = NULL;
    }
}

static void split_tabs(char *s, char **f, int maxf, int *nf)
{
    *nf = 0;
    while (*nf < maxf) {
        f[(*nf)++] = s;
        char *t = strchr(s, '\t');
        if (!t)
            break;
        *t = 0;
        s = t + 1;
    }
}

static int hexval(int c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    c |= 0x20;
    return c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1;
}

static void pct_decode(char *s)
{
    char *o = s;
    for (; *s; s++) {
        int h, l;
        if (*s == '%' && (h = hexval(s[1])) >= 0 && (l = hexval(s[2])) >= 0) {
            *o++ = (char)(h * 16 + l);
            s += 2;
        } else {
            *o++ = *s;
        }
    }
    *o = 0;
}

static void handle_directive(front_t *fr, char *dir)
{
    char *f[8];
    int nf = 0;
    while (nf < 8) {
        f[nf++] = dir;
        char *t = strchr(dir, '|');
        if (!t)
            break;
        *t = 0;
        dir = t + 1;
    }
    for (int i = 0; i < nf; i++)
        pct_decode(f[i]);
    channel_t *ch = nf >= 2 ? ch_find(f[1]) : NULL;
    if (!ch) {
        front_error(fr, 404, "Stream not found");
        return;
    }
    if (!strcmp(f[0], "ts")) {
        start_ts_client(fr, ch, nf >= 3 ? f[2] : "");
        return;
    }
    if (!strcmp(f[0], "hls")) {
        snprintf(fr->hls_pre, sizeof fr->hls_pre, "%s", nf >= 3 ? f[2] : "");
        snprintf(fr->hls_suf, sizeof fr->hls_suf, "%s", nf >= 4 ? f[3] : "");
        fr->head_only = nf >= 5 && !strcmp(f[4], "HEAD");
        ch->hls_last_req = g_now;
        if (!ch->hls_on)
            hls_out_activate(ch);
        if (ch->nsegs) {
            send_playlist(fr, ch);
            return;
        }
        if (fr->up)
            up_close(fr->up);
        fr->mode = FR_WAIT;
        fr->ch = ch;
        fr->wait_until = g_now + ch->seg_target + 2.5;
        fr->cnext = ch->waiters;
        if (ch->waiters)
            ch->waiters->cprev = fr;
        ch->waiters = fr;
        ev_watch(&fr->ev, EPOLLIN);
        return;
    }
    if (!strcmp(f[0], "seg") && nf >= 3) {
        ch->hls_last_req = g_now;
        long long n = -1;
        if (!strncmp(f[2], "segment-", 8))
            n = atoll(f[2] + 8);
        for (int i = 0; i < ch->nsegs; i++)
            if (ch->segs[i]->seq == n) {
                front_respond(fr, 200, "OK", "video/mp2t", "Cache-Control: public, max-age=60\r\n", NULL, 0,
                              ch->segs[i], nf >= 4 && !strcmp(f[3], "HEAD"));
                return;
            }
        front_error(fr, 404, "Segment not found or expired");
        return;
    }
    front_error(fr, 500, "Bad core directive");
}

/* ---- proxy to the Python control server */

static const char *hdr_skip[] = {"connection", "keep-alive", "proxy-connection", "x-forwarded-for",
                                 "x-astrag-via", "x-astrag-core", NULL};

static void up_on_event(up_t *u, uint32_t evs);

static void start_proxy(front_t *fr, size_t hl)
{
    char *h = (char *)fr->in.p;
    up_t *u = calloc(1, sizeof *u);
    if (!u) {
        front_close(fr);
        return;
    }
    u->ev.type = EV_UP;
    u->ev.obj = u;
    u->fr = fr;
    fr->up = u;
    fr->mode = FR_PROXY;
    char *eol = memchr(h, '\n', hl);
    size_t first = eol ? (size_t)(eol - h + 1) : hl;
    buf_add(&u->send, h, first);
    int upgrade = 0;
    char *line = h + first;
    char *end = h + hl - 2;
    while (line < end) {
        char *e = memchr(line, '\n', end - line);
        size_t ll = e ? (size_t)(e - line + 1) : (size_t)(end - line);
        char name[64];
        size_t nl = strcspn(line, ":\r\n");
        if (nl < sizeof name && nl < ll) {
            memcpy(name, line, nl);
            name[nl] = 0;
            int skip = 0;
            for (int i = 0; hdr_skip[i]; i++)
                if (!strcasecmp(name, hdr_skip[i]))
                    skip = 1;
            if (!strcasecmp(name, "upgrade"))
                upgrade = 1;
            if (!strcasecmp(name, "user-agent")) {
                const char *v = line + nl + 1;
                while (*v == ' ')
                    v++;
                size_t vl = strcspn(v, "\r\n");
                snprintf(fr->ua, sizeof fr->ua, "%.*s", (int)vl, v);
                clean_field(fr->ua);
            }
            if (!skip)
                buf_add(&u->send, line, ll);
        }
        line += ll;
    }
    fr->ws = upgrade;
    buf_fmt(&u->send, "Connection: %s\r\nX-Forwarded-For: %s\r\nX-Astrag-Via: %s\r\n\r\n",
            upgrade ? "Upgrade" : "close", fr->ip, g_secret);
    if (fr->in.len > hl)
        buf_add(&u->send, fr->in.p + hl, fr->in.len - hl);
    fr->in.len = 0;
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port = htons(g_upstream_port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    u->ev.fd = fd;
    if (fd < 0 || (connect(fd, (struct sockaddr *)&sa, sizeof sa) < 0 && errno != EINPROGRESS)) {
        front_error(fr, 502, "Control server unavailable");
        return;
    }
    u->connecting = 1;
    ev_watch(&u->ev, EPOLLOUT);
    ev_watch(&fr->ev, EPOLLIN);
    fr->deadline = g_now + 120;
}

static void up_flush_send(up_t *u)
{
    while (u->send_off < u->send.len) {
        ssize_t n = send(u->ev.fd, u->send.p + u->send_off, u->send.len - u->send_off, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                ev_watch(&u->ev, (u->paused ? 0 : EPOLLIN) | EPOLLOUT);
                return;
            }
            front_t *fr = u->fr;
            up_close(u);
            if (fr)
                front_close(fr);
            return;
        }
        u->send_off += n;
    }
    u->send.len = 0;
    u->send_off = 0;
    ev_watch(&u->ev, u->paused ? 0 : EPOLLIN);
}

static void up_on_event(up_t *u, uint32_t evs)
{
    front_t *fr = u->fr;
    if (!fr) {
        up_close(u);
        return;
    }
    if (u->connecting) {
        int err = 0;
        socklen_t l = sizeof err;
        getsockopt(u->ev.fd, SOL_SOCKET, SO_ERROR, &err, &l);
        if (err) {
            front_error(fr, 502, "Control server unavailable");
            return;
        }
        u->connecting = 0;
    }
    if (evs & EPOLLOUT)
        up_flush_send(u);
    if (u->ev.dead || !(evs & (EPOLLIN | EPOLLHUP | EPOLLERR)))
        return;
    uint8_t rb[65536];
    ssize_t n = recv(u->ev.fd, rb, sizeof rb, 0);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;
        n = 0;
    }
    if (n == 0) {
        u->eof = 1;
        int had_head = u->head_done;
        up_close(u);
        if (!had_head)
            front_error(fr, 502, "Bad gateway");
        else if (fr->out_off >= fr->out.len)
            front_close(fr);
        else
            fr->mode = FR_RESP;
        return;
    }
    if (!u->head_done) {
        buf_add(&u->hb, rb, n);
        uint8_t *e = memmem(u->hb.p, u->hb.len, "\r\n\r\n", 4);
        if (!e) {
            if (u->hb.len > 65536)
                front_error(fr, 502, "Bad gateway");
            return;
        }
        size_t hl = e - u->hb.p + 4;
        char *hd = NULL;
        for (size_t i = 0; i + 16 <= hl; i++)
            if (u->hb.p[i] == '\n' && !strncasecmp((char *)u->hb.p + i + 1, "x-astrag-core:", 14)) {
                hd = (char *)u->hb.p + i;
                break;
            }
        if (hd) {
            char *v = hd + 15;
            while (*v == ' ')
                v++;
            size_t vl = 0;
            while ((uint8_t *)v + vl < u->hb.p + hl && v[vl] != '\r' && v[vl] != '\n')
                vl++;
            char dir[2048];
            snprintf(dir, sizeof dir, "%.*s", (int)vl, v);
            up_close(u);
            fr->deadline = g_now + 60;
            handle_directive(fr, dir);
            return;
        }
        u->head_done = 1;
        buf_add(&fr->out, u->hb.p, u->hb.len);
        u->hb.len = 0;
    } else {
        buf_add(&fr->out, rb, n);
    }
    if (fr->out.len - fr->out_off > 1024 * 1024) {
        u->paused = 1;
        ev_watch(&u->ev, u->send_off < u->send.len ? EPOLLOUT : 0);
    }
    front_flush(fr);
}

static void front_on_event(front_t *fr, uint32_t evs)
{
    if (fr->mode == FR_TS) {
        if (evs & (EPOLLIN | EPOLLHUP | EPOLLERR)) {
            uint8_t tmp[4096];
            ssize_t n = recv(fr->ev.fd, tmp, sizeof tmp, 0);
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                front_close(fr);
                return;
            }
        }
        if (evs & EPOLLOUT)
            client_send(fr);
        return;
    }
    if (fr->mode == FR_WAIT || fr->mode == FR_RESP) {
        if (evs & (EPOLLIN | EPOLLHUP | EPOLLERR)) {
            uint8_t tmp[4096];
            ssize_t n = recv(fr->ev.fd, tmp, sizeof tmp, 0);
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                wait_remove(fr);
                front_close(fr);
                return;
            }
        }
        if ((evs & EPOLLOUT) && fr->mode == FR_RESP)
            front_flush(fr);
        return;
    }
    if (evs & EPOLLOUT)
        front_flush(fr);
    if (fr->ev.dead || !(evs & (EPOLLIN | EPOLLHUP | EPOLLERR)))
        return;
    uint8_t rb[65536];
    ssize_t n = recv(fr->ev.fd, rb, sizeof rb, 0);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;
        front_close(fr);
        return;
    }
    if (n == 0) {
        if (fr->mode == FR_HEAD || fr->ws) {
            front_close(fr);
            return;
        }
        fr->cl_eof = 1;
        ev_watch(&fr->ev, fr->out_off < fr->out.len ? EPOLLOUT : 0);
        return;
    }
    if (fr->mode == FR_HEAD) {
        buf_add(&fr->in, rb, n);
        uint8_t *e = memmem(fr->in.p, fr->in.len, "\r\n\r\n", 4);
        if (!e) {
            if (fr->in.len > 32768)
                front_error(fr, 431, "Request header too large");
            return;
        }
        start_proxy(fr, e - fr->in.p + 4);
        return;
    }
    if (fr->mode == FR_PROXY && fr->up) {
        up_t *u = fr->up;
        buf_add(&u->send, rb, n);
        if (!u->connecting)
            up_flush_send(u);
    }
}

/* ------------------------------------------------------------- listeners */

typedef struct listener {
    ev_t ev;
    char host[64];
    int port;
    struct listener *next;
} listener_t;
static listener_t *g_listeners;

static int listen_open(const char *host, int port)
{
    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    char ps[16];
    snprintf(ps, sizeof ps, "%d", port);
    if (getaddrinfo(host && *host ? host : NULL, ps, &hints, &res) || !res)
        return -1;
    int fd = socket(res->ai_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int one = 1;
    if (fd >= 0) {
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        if (bind(fd, res->ai_addr, res->ai_addrlen) < 0 || listen(fd, 2048) < 0) {
            close(fd);
            fd = -1;
        }
    }
    freeaddrinfo(res);
    return fd;
}

static int listener_add(const char *host, int port)
{
    for (listener_t *l = g_listeners; l; l = l->next)
        if (l->port == port)
            return 0;
    int fd = listen_open(host, port);
    if (fd < 0) {
        LOG("cannot listen on %s:%d: %s", host, port, strerror(errno));
        return -1;
    }
    listener_t *l = calloc(1, sizeof *l);
    l->ev.type = EV_LISTEN;
    l->ev.fd = fd;
    l->ev.obj = l;
    snprintf(l->host, sizeof l->host, "%s", host);
    l->port = port;
    l->next = g_listeners;
    g_listeners = l;
    ev_watch(&l->ev, EPOLLIN);
    return 0;
}

static void listener_del(int port)
{
    for (listener_t **pp = &g_listeners; *pp; pp = &(*pp)->next)
        if ((*pp)->port == port) {
            listener_t *l = *pp;
            *pp = l->next;
            l->ev.dead = 1;
            ev_close(&l->ev);
            bury(free, l);
            return;
        }
}

static void on_accept(listener_t *l)
{
    for (int i = 0; i < 64; i++) {
        struct sockaddr_storage ss;
        socklen_t sl = sizeof ss;
        int fd = accept4(l->ev.fd, (struct sockaddr *)&ss, &sl, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0)
            return;
        front_t *fr = calloc(1, sizeof *fr);
        if (!fr) {
            close(fd);
            continue;
        }
        fr->ev.type = EV_FRONT;
        fr->ev.fd = fd;
        fr->ev.obj = fr;
        fr->mode = FR_HEAD;
        fr->deadline = g_now + 20;
        if (ss.ss_family == AF_INET)
            inet_ntop(AF_INET, &((struct sockaddr_in *)&ss)->sin_addr, fr->ip, sizeof fr->ip);
        else if (ss.ss_family == AF_INET6) {
            struct sockaddr_in6 *s6 = (struct sockaddr_in6 *)&ss;
            if (IN6_IS_ADDR_V4MAPPED(&s6->sin6_addr))
                inet_ntop(AF_INET, &s6->sin6_addr.s6_addr[12], fr->ip, sizeof fr->ip);
            else
                inet_ntop(AF_INET6, &s6->sin6_addr, fr->ip, sizeof fr->ip);
        }
        fr->gnext = g_fronts;
        if (g_fronts)
            g_fronts->gprev = fr;
        g_fronts = fr;
        ev_watch(&fr->ev, EPOLLIN);
    }
}

/* ------------------------------------------------------------- HLS input */

typedef struct qseg {
    char *url, *key_url;
    double dur;
    long long seq;
    uint8_t iv[16];
    int iv_set;
    struct qseg *next;
} qseg_t;

typedef struct chunk {
    struct chunk *next;
    size_t len, off, cap;
    uint8_t d[];
} chunk_t;

typedef struct slot {
    struct slot *next;
    hls_t *h;
    fetch_t *f;
    qseg_t *q;
    chunk_t *ch_head, *ch_tail;
    size_t unconsumed, got;
    int64_t clen;
    int done, failed, need_key, fed;
    EVP_CIPHER_CTX *dec;
} slot_t;

struct hls {
    channel_t *ch;
    char master[4096], pl[4096];
    fetch_t *plf, *keyf;
    buf_t plbody, kbody;
    char key_fetch_url[4096];
    double next_poll, td, credit, last_pace;
    int first, pl_err, consec_err, burst_done;
    long long last_seq;
    qseg_t *qh, *qt;
    int qn;
    slot_t *sh, *st;
    int sn;
    uint64_t seen[256];
    int seen_i;
    double avg_bps;
    struct { char url[4096]; uint8_t key[16]; int ok; } keys[4];
    int key_i;
};

static void ch_input_fail(channel_t *ch, const char *msg);

static uint64_t fnv(const char *s, size_t n)
{
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; i++) {
        h ^= (uint8_t)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

/* Dedup key ignores volatile auth query parameters (same rule as the Python engine). */
static uint64_t seg_key(const char *url)
{
    const char *q = strchr(url, '?');
    if (!q)
        return fnv(url, strlen(url));
    if (q - url >= 3 && !strncasecmp(q - 3, ".ts", 3))
        return fnv(url, q - url);
    static const char *drop[] = {"token", "md5", "auth", "wmsauthsign", "expires", "vtoken", "signature",
                                 "hash", "key", NULL};
    uint64_t h = fnv(url, q - url);
    const char *p = q + 1;
    while (*p) {
        size_t n = strcspn(p, "&");
        size_t kl = strcspn(p, "=&");
        int skip = 0;
        for (int i = 0; drop[i]; i++)
            if (strlen(drop[i]) == kl && !strncasecmp(p, drop[i], kl))
                skip = 1;
        if (!skip)
            h ^= fnv(p, n) * 31;
        p += n;
        if (*p == '&')
            p++;
    }
    return h;
}

static int seen_has(hls_t *h, uint64_t k)
{
    for (int i = 0; i < 256; i++)
        if (h->seen[i] == k)
            return 1;
    return 0;
}

static void seen_add(hls_t *h, uint64_t k)
{
    h->seen[h->seen_i] = k;
    h->seen_i = (h->seen_i + 1) % 256;
}

static void qseg_free(qseg_t *q)
{
    if (!q)
        return;
    free(q->url);
    free(q->key_url);
    free(q);
}

static void slot_free(slot_t *s)
{
    fetch_cancel(s->f);
    while (s->ch_head) {
        chunk_t *c = s->ch_head;
        s->ch_head = c->next;
        free(c);
    }
    if (s->dec)
        EVP_CIPHER_CTX_free(s->dec);
    qseg_free(s->q);
    free(s);
}

static void hls_mem_free(void *p)
{
    hls_t *h = p;
    buf_free(&h->plbody);
    buf_free(&h->kbody);
    free(h);
}

static void hls_free(hls_t *h)
{
    if (!h)
        return;
    fetch_cancel(h->plf);
    fetch_cancel(h->keyf);
    h->plf = h->keyf = NULL;
    while (h->sh) {
        slot_t *s = h->sh;
        h->sh = s->next;
        slot_free(s);
    }
    h->st = NULL;
    h->sn = 0;
    while (h->qh) {
        qseg_t *q = h->qh;
        h->qh = q->next;
        qseg_free(q);
    }
    h->qt = NULL;
    h->qn = 0;
    bury(hls_mem_free, h);
}

static size_t hls_total(hls_t *h)
{
    size_t t = 0;
    for (slot_t *s = h->sh; s; s = s->next)
        t += s->unconsumed;
    return t;
}

static double hls_rate(hls_t *h)
{
    slot_t *s = h->sh;
    if (s && s->clen > 0 && s->q->dur > 0.2)
        return s->clen / s->q->dur;
    if (h->avg_bps > 0)
        return h->avg_bps;
    if (h->ch->pcr_bitrate > 100000)
        return h->ch->pcr_bitrate / 8;
    return 500000;
}

static size_t hls_cap(hls_t *h)
{
    double bps = h->avg_bps > 0 ? h->avg_bps : hls_rate(h);
    double want = bps * (h->td + 3.0);
    return (size_t)clampd(want, 3.0 * 1024 * 1024, 24.0 * 1024 * 1024);
}

static void hls_check_pause(hls_t *h)
{
    /* The head segment always keeps downloading: pausing it while a lookahead fills the cap would deadlock. */
    int full = hls_total(h) >= hls_cap(h);
    for (slot_t *s = h->sh; s; s = s->next)
        if (s->f)
            fetch_pause(s->f, full && (s != h->sh || h->sn == 1));
}

static void slot_append(slot_t *s, const uint8_t *d, size_t n)
{
    while (n) {
        chunk_t *c = s->ch_tail;
        if (!c || c->len == c->cap) {
            size_t cap = 65424; /* 348 * 188B: exact TS packet multiple avoids carry splits across chunks */
            c = malloc(sizeof *c + cap);
            if (!c)
                return;
            c->next = NULL;
            c->len = c->off = 0;
            c->cap = cap;
            if (s->ch_tail)
                s->ch_tail->next = c;
            else
                s->ch_head = c;
            s->ch_tail = c;
        }
        size_t t = min_sz(n, c->cap - c->len);
        memcpy(c->d + c->len, d, t);
        c->len += t;
        d += t;
        n -= t;
        s->unconsumed += t;
    }
}

static const uint8_t *key_get(hls_t *h, const char *url)
{
    for (int i = 0; i < 4; i++)
        if (h->keys[i].ok && !strcmp(h->keys[i].url, url))
            return h->keys[i].key;
    return NULL;
}

static void slot_start(hls_t *h, slot_t *s);
static void hls_pump(hls_t *h);

static void seg_head(fetch_t *f)
{
    slot_t *s = f->owner;
    s->clen = f->status == 200 ? f->clen : -1;
}

static void seg_data(fetch_t *f, const uint8_t *d, size_t n)
{
    slot_t *s = f->owner;
    if (f->status != 200 && f->status != 206)
        return;
    if (s->got + n > 96u * 1024 * 1024) {
        fetch_cancel(f);
        s->f = NULL;
        s->done = 1;
        return;
    }
    s->got += n;
    if (s->dec) {
        static uint8_t out[65536 + 32];
        while (n) {
            size_t t = min_sz(n, 65536);
            int ol = 0;
            EVP_DecryptUpdate(s->dec, out, &ol, d, (int)t);
            slot_append(s, out, ol);
            d += t;
            n -= t;
        }
    } else {
        slot_append(s, d, n);
    }
    hls_check_pause(s->h);
}

static void seg_done(fetch_t *f, int ok, const char *err)
{
    slot_t *s = f->owner;
    hls_t *h = s->h;
    channel_t *ch = h->ch;
    s->f = NULL;
    s->done = 1;
    if (s->dec && f->status == 200) {
        uint8_t out[64];
        int ol = 0;
        if (EVP_DecryptFinal_ex(s->dec, out, &ol) == 1)
            slot_append(s, out, ol);
    }
    if (f->status == 404 || f->status == 410) {
        if (f->status == 410 && strcmp(h->pl, h->master))
            snprintf(h->pl, sizeof h->pl, "%s", h->master);
        return;
    }
    if (f->status == 401 || f->status == 403) {
        if (strcmp(h->pl, h->master)) {
            snprintf(h->pl, sizeof h->pl, "%s", h->master);
            h->next_poll = g_now;
        }
    }
    if (f->status == 200 && s->got >= TS) {
        if (s->q->dur > 0.2) {
            double m = s->got / s->q->dur;
            h->avg_bps = h->avg_bps > 0 ? 0.7 * h->avg_bps + 0.3 * m : m;
        }
        h->consec_err = 0;
        return;
    }
    s->failed = 1;
    ch->input_errors++;
    snprintf(ch->last_error, sizeof ch->last_error, "segment: %s",
             f->status && f->status != 200 ? "HTTP error" : (err ? err : "empty"));
    if (f->status && f->status != 200)
        snprintf(ch->last_error, sizeof ch->last_error, "segment HTTP %d", f->status);
    (void)ok;
    if (++h->consec_err >= 10)
        ch_input_fail(ch, ch->last_error);
}

static const fetch_cb_t g_seg_cb = {seg_head, seg_data, seg_done};

static void key_head(fetch_t *f) { (void)f; }
static void key_data(fetch_t *f, const uint8_t *d, size_t n)
{
    hls_t *h = f->owner;
    if (h->kbody.len + n <= 64)
        buf_add(&h->kbody, d, n);
}

static void key_done(fetch_t *f, int ok, const char *err)
{
    (void)err;
    hls_t *h = f->owner;
    h->keyf = NULL;
    int good = ok && f->status == 200 && h->kbody.len == 16;
    if (good) {
        int i = h->key_i;
        h->key_i = (h->key_i + 1) % 4;
        snprintf(h->keys[i].url, sizeof h->keys[i].url, "%s", h->key_fetch_url);
        memcpy(h->keys[i].key, h->kbody.p, 16);
        h->keys[i].ok = 1;
    }
    h->kbody.len = 0;
    for (slot_t *s = h->sh; s; s = s->next) {
        if (!s->need_key)
            continue;
        if (good || strcmp(s->q->key_url, h->key_fetch_url)) {
            s->need_key = 0;
            slot_start(h, s);
        } else {
            s->need_key = 0;
            s->done = s->failed = 1;
        }
        if (!h->ch->hls || h->ch->hls != h)
            return;
    }
}

static const fetch_cb_t g_key_cb = {key_head, key_data, key_done};

static void slot_start(hls_t *h, slot_t *s)
{
    channel_t *ch = h->ch;
    qseg_t *q = s->q;
    if (q->key_url) {
        const uint8_t *k = key_get(h, q->key_url);
        if (!k) {
            s->need_key = 1;
            if (!h->keyf) {
                snprintf(h->key_fetch_url, sizeof h->key_fetch_url, "%s", q->key_url);
                h->kbody.len = 0;
                h->keyf = fetch_start(q->key_url, ch->ua, &g_key_cb, h, 8, 10);
            }
            return;
        }
        uint8_t iv[16];
        if (q->iv_set)
            memcpy(iv, q->iv, 16);
        else {
            memset(iv, 0, 16);
            for (int i = 0; i < 8; i++)
                iv[15 - i] = (uint8_t)((unsigned long long)q->seq >> (8 * i));
        }
        s->dec = EVP_CIPHER_CTX_new();
        EVP_DecryptInit_ex(s->dec, EVP_aes_128_cbc(), NULL, k, iv);
    }
    double idle = clampd(h->td * 1.5 + 2.0, 5.0, 20.0);
    s->f = fetch_start(q->url, ch->ua, &g_seg_cb, s, 8.0, idle);
    if (!s->f)
        s->done = s->failed = 1;
}

static void hls_pump(hls_t *h)
{
    while (h->sn < g_hls_parallel && h->qh) {
        if (h->sn >= 1 && hls_total(h) >= hls_cap(h))
            break;
        qseg_t *q = h->qh;
        h->qh = q->next;
        if (!h->qh)
            h->qt = NULL;
        h->qn--;
        q->next = NULL;
        slot_t *s = calloc(1, sizeof *s);
        if (!s) {
            qseg_free(q);
            continue;
        }
        s->h = h;
        s->q = q;
        s->clen = -1;
        if (h->st)
            h->st->next = s;
        else
            h->sh = s;
        h->st = s;
        h->sn++;
        slot_start(h, s);
        if (h->ch->hls != h)
            return;
    }
}

static void pl_head(fetch_t *f) { (void)f; }
static void pl_data(fetch_t *f, const uint8_t *d, size_t n)
{
    hls_t *h = f->owner;
    if (h->plbody.len + n < 16u * 1024 * 1024)
        buf_add(&h->plbody, d, n);
}

static void hls_parse(hls_t *h, char *text, const char *base);

static void hls_pl_error(hls_t *h, const char *msg, int status)
{
    channel_t *ch = h->ch;
    ch->input_errors++;
    if (status)
        snprintf(ch->last_error, sizeof ch->last_error, "playlist HTTP %d", status);
    else
        snprintf(ch->last_error, sizeof ch->last_error, "playlist: %s", msg);
    if (strcmp(h->pl, h->master))
        snprintf(h->pl, sizeof h->pl, "%s", h->master);
    h->pl_err++;
    if (h->first || h->pl_err >= 10) {
        ch_input_fail(ch, ch->last_error);
        return;
    }
    h->next_poll = g_now + fmin(2.0, 0.3 * h->pl_err);
}

static void pl_done(fetch_t *f, int ok, const char *err)
{
    hls_t *h = f->owner;
    h->plf = NULL;
    if (!ok || f->status != 200) {
        hls_pl_error(h, err ? err : "failed", f->status != 200 ? f->status : 0);
        return;
    }
    buf_add(&h->plbody, "", 1);
    hls_parse(h, (char *)h->plbody.p, f->url);
}

static const fetch_cb_t g_pl_cb = {pl_head, pl_data, pl_done};

static int attr_get(const char *line, const char *name, char *out, size_t outsz)
{
    size_t nl = strlen(name);
    for (const char *p = line; (p = strcasestr(p, name)); p += nl) {
        if (p > line && p[-1] != ',' && p[-1] != ':' && p[-1] != ' ')
            continue;
        if (p[nl] != '=')
            continue;
        const char *v = p + nl + 1;
        size_t n;
        if (*v == '"') {
            v++;
            n = strcspn(v, "\"");
        } else
            n = strcspn(v, ",\r\n");
        snprintf(out, outsz, "%.*s", (int)n, v);
        return 1;
    }
    return 0;
}

typedef struct { char *uri, *key; double dur; long long seq; uint8_t iv[16]; int ivset; } ent_t;

static void hls_parse(hls_t *h, char *text, const char *base)
{
    channel_t *ch = h->ch;
    while (*text == ' ' || *text == '\r' || *text == '\n' || *text == '\t' || (uint8_t)*text == 0xEF ||
           (uint8_t)*text == 0xBB || (uint8_t)*text == 0xBF)
        text++;
    if (strncmp(text, "#EXTM3U", 7)) {
        hls_pl_error(h, "not an M3U8 playlist", 0);
        return;
    }
    if (strstr(text, "#EXT-X-STREAM-INF")) {
        long best = -1;
        char best_uri[4096] = "";
        int pending = 0;
        long bw = 0;
        for (char *line = strtok(text, "\r\n"); line; line = strtok(NULL, "\r\n")) {
            while (*line == ' ')
                line++;
            if (!strncmp(line, "#EXT-X-STREAM-INF", 17)) {
                char v[64];
                bw = attr_get(line, "BANDWIDTH", v, sizeof v) ? atol(v) : 0;
                pending = 1;
            } else if (pending && *line && *line != '#') {
                if (bw > best) {
                    best = bw;
                    snprintf(best_uri, sizeof best_uri, "%s", line);
                }
                pending = 0;
            }
        }
        if (!best_uri[0]) {
            hls_pl_error(h, "master playlist without variants", 0);
            return;
        }
        char u[4096];
        url_resolve(base, best_uri, u, sizeof u);
        snprintf(h->pl, sizeof h->pl, "%s", u);
        h->next_poll = g_now;
        return;
    }
    ent_t ring[16];
    long long cnt = 0, mseq = 0;
    int have_mseq = 0;
    double td = 0, dur = -1;
    char keyu[4096] = "";
    uint8_t iv[16];
    int ivset = 0;
    for (char *line = strtok(text, "\r\n"); line; line = strtok(NULL, "\r\n")) {
        while (*line == ' ' || *line == '\t')
            line++;
        if (!*line)
            continue;
        if (line[0] == '#') {
            if (!strncmp(line, "#EXT-X-TARGETDURATION:", 22))
                td = atof(line + 22);
            else if (!strncmp(line, "#EXT-X-MEDIA-SEQUENCE:", 22)) {
                mseq = atoll(line + 22);
                have_mseq = 1;
            } else if (!strncmp(line, "#EXTINF:", 8))
                dur = atof(line + 8);
            else if (!strncmp(line, "#EXT-X-KEY:", 11)) {
                char m[32] = "", uri[4096] = "", ivs[64] = "";
                attr_get(line, "METHOD", m, sizeof m);
                if (!strcasecmp(m, "AES-128") && attr_get(line, "URI", uri, sizeof uri)) {
                    url_resolve(base, uri, keyu, sizeof keyu);
                    ivset = 0;
                    if (attr_get(line, "IV", ivs, sizeof ivs) && (ivs[1] == 'x' || ivs[1] == 'X') &&
                        strlen(ivs) >= 34) {
                        for (int i = 0; i < 16; i++) {
                            unsigned v;
                            sscanf(ivs + 2 + 2 * i, "%2x", &v);
                            iv[i] = (uint8_t)v;
                        }
                        ivset = 1;
                    }
                } else {
                    keyu[0] = 0;
                }
            }
            continue;
        }
        ent_t *e = &ring[cnt % 16];
        if (cnt >= 16)
            free(e->key);
        e->uri = line;
        e->dur = dur;
        e->seq = mseq + cnt;
        e->key = keyu[0] ? strdup(keyu) : NULL;
        memcpy(e->iv, iv, 16);
        e->ivset = ivset;
        cnt++;
        dur = -1;
    }
    if (td > 0)
        h->td = td;
    int n = cnt < 16 ? (int)cnt : 16;
    ent_t list[16];
    for (int i = 0; i < n; i++)
        list[i] = ring[(cnt - n + i) % 16];
    int isnew[16], nnew = 0;
    if (have_mseq && n && h->last_seq >= 0 && list[n - 1].seq < h->last_seq - 50)
        h->last_seq = -1;
    for (int i = 0; i < n; i++) {
        char u[4096];
        url_resolve(base, list[i].uri, u, sizeof u);
        uint64_t k = seg_key(u);
        isnew[i] = have_mseq ? list[i].seq > h->last_seq : !seen_has(h, k);
        if (isnew[i] && !have_mseq && seen_has(h, k))
            isnew[i] = 0;
        nnew += isnew[i];
    }
    int limit = h->first ? (h->td <= 6.0 ? 3 : 2) : (h->td <= 6.0 ? 3 : 2);
    int skip = nnew > limit ? nnew - limit : 0;
    h->first = 0;
    int added = 0;
    for (int i = 0; i < n; i++) {
        if (!isnew[i])
            continue;
        char u[4096];
        url_resolve(base, list[i].uri, u, sizeof u);
        seen_add(h, seg_key(u));
        if (list[i].seq > h->last_seq)
            h->last_seq = list[i].seq;
        if (skip > 0) {
            skip--;
            continue;
        }
        if (h->qn >= 24)
            continue;
        qseg_t *q = calloc(1, sizeof *q);
        if (!q)
            continue;
        q->url = strdup(u);
        q->key_url = list[i].key ? strdup(list[i].key) : NULL;
        q->dur = list[i].dur > 0 ? list[i].dur : h->td;
        q->seq = list[i].seq;
        memcpy(q->iv, list[i].iv, 16);
        q->iv_set = list[i].ivset;
        if (h->qt)
            h->qt->next = q;
        else
            h->qh = q;
        h->qt = q;
        h->qn++;
        added++;
    }
    for (int i = 0; i < 16 && i < cnt; i++)
        free(ring[i].key);
    h->pl_err = 0;
    double td2 = h->td > 0 ? h->td : 6.0;
    h->next_poll = g_now + (added ? clampd(td2 * 0.42, 0.3, 1.8) : clampd(td2 * 0.22, 0.4, 1.1));
    (void)ch;
    hls_pump(h);
}

static void hls_start(channel_t *ch, const char *master, const char *pl, char *body)
{
    hls_t *h = calloc(1, sizeof *h);
    if (!h) {
        ch_input_fail(ch, "out of memory");
        return;
    }
    h->ch = ch;
    snprintf(h->master, sizeof h->master, "%s", master);
    snprintf(h->pl, sizeof h->pl, "%s", pl);
    h->first = 1;
    h->last_seq = -1;
    h->td = 6.0;
    h->last_pace = g_now;
    h->next_poll = g_now;
    ch->hls = h;
    ch->mode = IN_HLS;
    if (body)
        hls_parse(h, body, pl);
}

static void hls_pace(hls_t *h)
{
    channel_t *ch = h->ch;
    double dt = g_now - h->last_pace;
    h->last_pace = g_now;
    dt = clampd(dt, 0, 0.5);
    double bps = hls_rate(h);
    size_t total = hls_total(h);
    double buf_sec = total / bps;
    double hi = fmax(6.5, h->td + 1.5), mid = fmax(4.2, h->td * 0.75 + 0.5);
    double factor = buf_sec > hi ? 1.04 : buf_sec > mid ? 1.015 : buf_sec >= 1.0 ? 1.0 : 0.992;
    if (!h->burst_done && total >= TS * 64) {
        h->credit += fmin(bps * 1.0, 3.0 * 1024 * 1024);
        h->burst_done = 1;
    }
    h->credit += dt * bps * factor;
    double cap = fmax(bps * 0.3, 65536.0);
    while (h->sh) {
        slot_t *s = h->sh;
        chunk_t *c = s->ch_head;
        if (!c) {
            if (s->done && !s->need_key) {
                h->sh = s->next;
                if (!h->sh)
                    h->st = NULL;
                h->sn--;
                slot_free(s);
                continue;
            }
            break;
        }
        size_t avail = c->len - c->off;
        size_t want = h->credit > 0 ? (size_t)h->credit : 0;
        want -= want % TS;
        if (!want)
            break;
        /* Coalesce feeds into >= 64 KB bursts when no live .ts viewers are attached to reduce CPU wakeups */
        if (ch->nclients == 0 && want < 65424 && want < avail && (!s->done || c->next))
            break;
        if (!s->fed) {
            s->fed = 1;
            memset(ch->in_cc, -1, sizeof ch->in_cc);
        }
        size_t nfeed = min_sz(avail, want);
        ts_feed(ch, c->d + c->off, nfeed, 1);
        c->off += nfeed;
        s->unconsumed -= nfeed;
        h->credit -= nfeed;
        if (c->off >= c->len) {
            s->ch_head = c->next;
            if (!s->ch_head)
                s->ch_tail = NULL;
            free(c);
        }
    }
    if (h->credit > cap && hls_total(h) == 0)
        h->credit = cap;
    if (h->credit < 0)
        h->credit = 0;
    hls_check_pause(h);
    hls_pump(h);
}

static void hls_tick(hls_t *h)
{
    if (!h->plf && g_now >= h->next_poll) {
        if (h->qn >= 6) {
            h->next_poll = g_now + 0.25;
        } else {
            h->plbody.len = 0;
            h->plf = fetch_start(h->pl, h->ch->ua, &g_pl_cb, h, 8.0, 10.0);
            if (!h->plf) {
                hls_pl_error(h, "bad URL", 0);
                return;
            }
        }
    }
    if (h->ch->hls != h)
        return;
    hls_pace(h);
}

/* ---------------------------------------------------------------- inputs */

static void ch_input_stop(channel_t *ch)
{
    fetch_cancel(ch->tsf);
    ch->tsf = NULL;
    if (ch->hls) {
        hls_t *h = ch->hls;
        ch->hls = NULL;
        hls_free(h);
    }
    buf_free(&ch->probe);
    ch->mode = IN_WAIT;
}

static void ch_input_fail(channel_t *ch, const char *msg)
{
    if (msg && msg != ch->last_error)
        snprintf(ch->last_error, sizeof ch->last_error, "%s", msg);
    LOG("%s: input %d failed: %s", ch->id, ch->cur + 1, ch->last_error);
    ch_input_stop(ch);
    ch->fails_row++;
    int next = (ch->cur + 1) % ch->nurls;
    if (next != ch->cur) {
        ts_reset_psi(ch);
        memset(ch->out_cc, -1, sizeof ch->out_cc);
        ch->disc_pending = 1;
    } else {
        ch->carry_n = 0;
        ch->synced = 0;
        memset(ch->in_cc, -1, sizeof ch->in_cc);
    }
    ch->cur = next;
    ch->retry_at = g_now + (ch->fails_row >= ch->nurls ? 2.0 : 0.5);
}

static void ts_in_head(fetch_t *f)
{
    channel_t *ch = f->owner;
    if (f->status == 200 && strcasestr(f->ctype, "mpegurl"))
        ch->probe_m3u = 1;
}

static void ts_in_data(fetch_t *f, const uint8_t *d, size_t n)
{
    channel_t *ch = f->owner;
    if (f->status != 200)
        return;
    if (!ch->ts_checked) {
        ch->ts_checked = 1;
        size_t i = 0;
        while (i < n && (d[i] == ' ' || d[i] == '\r' || d[i] == '\n' || d[i] == 0xEF || d[i] == 0xBB ||
                         d[i] == 0xBF))
            i++;
        if (n - i >= 7 && !memcmp(d + i, "#EXTM3U", 7))
            ch->probe_m3u = 1;
    }
    if (ch->probe_m3u) {
        if (ch->probe.len + n > 16u * 1024 * 1024) {
            ch_input_fail(ch, "playlist too large");
            return;
        }
        buf_add(&ch->probe, d, n);
        return;
    }
    ts_feed(ch, d, n, 0);
}

static void ts_in_done(fetch_t *f, int ok, const char *err)
{
    channel_t *ch = f->owner;
    ch->tsf = NULL;
    if (f->status && f->status != 200) {
        char m[64];
        snprintf(m, sizeof m, "HTTP %d", f->status);
        ch->input_errors++;
        ch_input_fail(ch, m);
        return;
    }
    if (ch->probe_m3u && ch->probe.len) {
        buf_add(&ch->probe, "", 1);
        char *body = (char *)ch->probe.p;
        ch->probe.p = NULL;
        ch->probe.len = ch->probe.cap = 0;
        char master[4096];
        snprintf(master, sizeof master, "%s", ch->urls[ch->cur]);
        master[strcspn(master, "#")] = 0;
        hls_start(ch, master, f->url, body);
        free(body);
        return;
    }
    ch->input_errors++;
    ch_input_fail(ch, err ? err : (ok ? "stream ended" : "stream error"));
}

static const fetch_cb_t g_ts_cb = {ts_in_head, ts_in_data, ts_in_done};

static void ch_input_start(channel_t *ch)
{
    char url[4096];
    snprintf(url, sizeof url, "%s", ch->urls[ch->cur]);
    char *hash = strchr(url, '#');
    snprintf(ch->ua, sizeof ch->ua, "AstraG/2.0");
    ch->drop_sdt = ch->drop_eit = 0;
    if (hash) {
        *hash = 0;
        for (char *opt = strtok(hash + 1, "&"); opt; opt = strtok(NULL, "&")) {
            if (!strncmp(opt, "ua=", 3))
                snprintf(ch->ua, sizeof ch->ua, "%s", opt + 3);
            else if (!strcmp(opt, "no_sdt"))
                ch->drop_sdt = 1;
            else if (!strcmp(opt, "no_eit"))
                ch->drop_eit = 1;
        }
    }
    ch->in_started = g_now;
    ch->last_data = g_now;
    ch->ts_checked = 0;
    ch->probe_m3u = 0;
    ch->probe.len = 0;
    size_t pl = strcspn(url, "?");
    if ((pl >= 5 && !strncasecmp(url + pl - 5, ".m3u8", 5)) || (pl >= 4 && !strncasecmp(url + pl - 4, ".m3u", 4))) {
        hls_start(ch, url, url, NULL);
        return;
    }
    ch->mode = IN_TS;
    double idle = ch->nurls > 1 ? 12.0 : 20.0;
    ch->tsf = fetch_start(url, ch->ua, &g_ts_cb, ch, ch->nurls > 1 ? 4.5 : 8.0, idle);
    if (!ch->tsf)
        ch_input_fail(ch, "cannot start request");
}

/* ---------------------------------------------------------------- channels */

static void ch_free(void *p)
{
    channel_t *ch = p;
    for (int i = 0; i < ch->nurls; i++)
        free(ch->urls[i]);
    free(ch->ring);
    buf_free(&ch->probe);
    free(ch);
}

static void ch_del(channel_t *ch)
{
    for (channel_t **pp = &g_channels; *pp; pp = &(*pp)->next)
        if (*pp == ch) {
            *pp = ch->next;
            break;
        }
    ch_input_stop(ch);
    while (ch->clients)
        front_close(ch->clients);
    while (ch->waiters) {
        front_t *w = ch->waiters;
        wait_remove(w);
        front_error(w, 404, "Stream stopped");
    }
    hls_out_off(ch);
    bury(ch_free, ch);
}

static void ch_add(char **f, int nf)
{
    /* ADD id seg_dur seg_keep name url... */
    if (nf < 6)
        return;
    channel_t *old = ch_find(f[1]);
    if (old)
        ch_del(old);
    channel_t *ch = calloc(1, sizeof *ch);
    if (!ch)
        return;
    snprintf(ch->id, sizeof ch->id, "%s", f[1]);
    ch->seg_target = clampd(atof(f[2]), 1.0, 10.0);
    ch->seg_keep = (int)clampd(atoi(f[3]), 2, 12);
    snprintf(ch->name, sizeof ch->name, "%s", f[4]);
    for (int i = 5; i < nf && ch->nurls < MAX_URLS; i++)
        if (*f[i])
            ch->urls[ch->nurls++] = strdup(f[i]);
    if (!ch->nurls) {
        free(ch);
        return;
    }
    ch->rcap = TS * 22310; /* ~4 MB until the bitrate is known */
    ch->ring = malloc(ch->rcap);
    if (!ch->ring) {
        ch_free(ch);
        return;
    }
    memset(ch->out_cc, -1, sizeof ch->out_cc);
    memset(ch->hls_cc, -1, sizeof ch->hls_cc);
    ts_reset_psi(ch);
    ch->win_start = g_now;
    ch->next_resize = g_now + 5;
    ch->hls_on = 1;
    ch->hls_last_req = -1e9;
    ch->next = g_channels;
    g_channels = ch;
    ch_input_start(ch);
}

static void ch_tick(channel_t *ch, int second)
{
    if (ch->mode == IN_WAIT) {
        if (g_now >= ch->retry_at)
            ch_input_start(ch);
    } else {
        double timeout = 10.0;
        if (ch->mode == IN_HLS && ch->hls)
            timeout = fmax(10.0, ch->hls->td * 2.5);
        if (g_now - ch->last_data > timeout) {
            ch->input_errors++;
            ch_input_fail(ch, "no data");
        } else if (ch->mode == IN_HLS && ch->hls) {
            hls_tick(ch->hls);
        }
        if (ch->last_data > ch->in_started && g_now - ch->in_started > 5.0)
            ch->fails_row = 0;
    }
    if (ch->clients && g_now - ch->last_flush >= 0.04) {
        ch->last_flush = g_now;
        for (front_t *c = ch->clients, *nx; c; c = nx) {
            nx = c->cnext;
            if (!c->blocked)
                client_send(c);
            else if (g_now - c->last_progress > 30.0)
                front_close(c);
        }
    }
    for (front_t *w = ch->waiters, *nx; w; w = nx) {
        nx = w->cnext;
        if (ch->nsegs) {
            wait_remove(w);
            send_playlist(w, ch);
        } else if (g_now > w->wait_until) {
            wait_remove(w);
            front_error(w, 503, "HLS segments not yet available");
        }
    }
    if (second) {
        double el = g_now - ch->win_start;
        if (el > 0.2)
            ch->bitrate = ch->win_bytes * 8.0 / el;
        ch->win_bytes = 0;
        ch->win_start = g_now;
        ch->scrambled = ch->scr_win;
        ch->scr_win = 0;
        if (g_now >= ch->next_resize) {
            ch->next_resize = g_now + 5;
            double bps = fmax(ch->bitrate, ch->pcr_bitrate) / 8;
            /* Whole GOP + start burst + slack: with 10 s GOPs an 8 s ring held no keyframe old enough. */
            double secs = fmax(8.0, ch->gop + START_BURST_S + 1.5);
            size_t want = (size_t)clampd(bps * secs, 2.0 * 1024 * 1024, 32.0 * 1024 * 1024);
            if (want > ch->rcap * 1.25 || want < ch->rcap * 0.6)
                ring_resize(ch, want);
        }
        char pids[1024];
        size_t pl = 0;
        pids[0] = 0;
        for (int pid = 0; pid < 8191 && pl < sizeof pids - 32; pid++)
            if (ch->kind[pid] && ch->kind[pid] != K_PMT)
                pl += snprintf(pids + pl, sizeof pids - pl, "%s%d:%d:%d", pl ? "," : "", pid, ch->kind[pid],
                               ch->stype[pid]);
        char pmts[256];
        size_t ml = 0;
        pmts[0] = 0;
        for (int i = 0; i < ch->npmt; i++)
            ml += snprintf(pmts + ml, sizeof pmts - ml, "%s%d:%d", ml ? "," : "", ch->pmt_pid[i], ch->pnr[i]);
        char err[256];
        snprintf(err, sizeof err, "%s", ch->last_error);
        clean_field(err);
        ctl_send("STAT\t%s\t%d\t%.0f\t%llu\t%llu\t%d\t%d\t%llu\t%d\t%d\t%s\t%s\t%llu\t%s\n", ch->id,
                 ch->bitrate > 0 ? 1 : 0, ch->bitrate, (unsigned long long)ch->cc_err,
                 (unsigned long long)ch->pes_err, ch->scrambled, ch->cur + 1, (unsigned long long)ch->packets,
                 ch->pcr_pid, ch->tsid, pids, pmts, ch->input_errors, err);
    }
}

/* ----------------------------------------------------------------- control */

static void ctl_flush(void)
{
    ctl_t *c = g_ctl;
    if (!c || c->ev.dead)
        return;
    while (c->out.len) {
        ssize_t n = send(c->ev.fd, c->out.p, c->out.len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                ev_watch(&c->ev, EPOLLIN | EPOLLOUT);
                return;
            }
            return;
        }
        buf_drop(&c->out, n);
    }
    ev_watch(&c->ev, EPOLLIN);
}

static void ctl_free(void *p)
{
    ctl_t *c = p;
    buf_free(&c->in);
    buf_free(&c->out);
    free(c);
}

static void ctl_close(ctl_t *c)
{
    if (c->ev.dead)
        return;
    c->ev.dead = 1;
    ev_close(&c->ev);
    if (g_ctl == c)
        g_ctl = NULL;
    bury(ctl_free, c);
}

static void ctl_line(ctl_t *c, char *line)
{
    char *f[MAX_URLS + 8];
    int nf;
    split_tabs(line, f, MAX_URLS + 8, &nf);
    if (!c->authed) {
        if (nf >= 2 && !strcmp(f[0], "AUTH") && !strcmp(f[1], g_secret)) {
            c->authed = 1;
            buf_str(&c->out, "OK\n");
            ctl_flush();
        } else {
            ctl_close(c);
        }
        return;
    }
    if (!strcmp(f[0], "ADD"))
        ch_add(f, nf);
    else if (!strcmp(f[0], "DEL") && nf >= 2) {
        channel_t *ch = ch_find(f[1]);
        if (ch)
            ch_del(ch);
    } else if (!strcmp(f[0], "KILL") && nf >= 2) {
        unsigned long long id = strtoull(f[1], NULL, 10);
        for (channel_t *ch = g_channels; ch; ch = ch->next)
            for (front_t *x = ch->clients; x; x = x->cnext)
                if (x->sess == id) {
                    front_close(x);
                    return;
                }
    } else if (!strcmp(f[0], "LISTEN") && nf >= 3) {
        listener_add(f[1], atoi(f[2]));
    } else if (!strcmp(f[0], "UNLISTEN") && nf >= 2) {
        listener_del(atoi(f[1]));
    }
}

static void ctl_on_event(ctl_t *c, uint32_t evs)
{
    if (evs & EPOLLOUT)
        ctl_flush();
    if (!(evs & (EPOLLIN | EPOLLHUP | EPOLLERR)))
        return;
    uint8_t rb[65536];
    ssize_t n = recv(c->ev.fd, rb, sizeof rb, 0);
    if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
        ctl_close(c);
        return;
    }
    if (n < 0)
        return;
    buf_add(&c->in, rb, n);
    size_t start = 0;
    for (size_t i = 0; i < c->in.len && !c->ev.dead; i++)
        if (c->in.p[i] == '\n') {
            c->in.p[i] = 0;
            if (i > start && c->in.p[i - 1] == '\r')
                c->in.p[i - 1] = 0;
            ctl_line(c, (char *)c->in.p + start);
            start = i + 1;
        }
    if (!c->ev.dead)
        buf_drop(&c->in, start);
}

static void ctl_accept(void)
{
    int fd = accept4(g_ctl_lev.fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0)
        return;
    if (g_ctl)
        ctl_close(g_ctl);
    ctl_t *c = calloc(1, sizeof *c);
    c->ev.type = EV_CTL;
    c->ev.fd = fd;
    c->ev.obj = c;
    g_ctl = c;
    ev_watch(&c->ev, EPOLLIN);
}

/* ------------------------------------------------------------------- main */

static void front_sweep(void)
{
    for (front_t *fr = g_fronts, *nx; fr; fr = nx) {
        nx = fr->gnext;
        if (fr->ev.dead)
            continue;
        if ((fr->mode == FR_HEAD || fr->mode == FR_RESP) && g_now > fr->deadline)
            front_close(fr);
    }
}

static volatile sig_atomic_t g_stop;
static void on_term(int s)
{
    (void)s;
    g_stop = 1;
}

int main(int argc, char **argv)
{
    const char *ctl_name = NULL;
    const char *listens[16];
    int nl = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--ctl") && i + 1 < argc)
            ctl_name = argv[++i];
        else if (!strcmp(argv[i], "--secret") && i + 1 < argc)
            snprintf(g_secret, sizeof g_secret, "%s", argv[++i]);
        else if (!strcmp(argv[i], "--upstream") && i + 1 < argc)
            g_upstream_port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--hls-parallel") && i + 1 < argc)
            g_hls_parallel = atoi(argv[++i]) == 2 ? 2 : 1;
        else if (!strcmp(argv[i], "--listen") && i + 1 < argc && nl < 16)
            listens[nl++] = argv[++i];
        else if (!strcmp(argv[i], "--version")) {
            puts("astrag-core 1.3");
            return 0;
        }
    }
    if (!ctl_name || !g_secret[0] || !g_upstream_port || !nl) {
        fprintf(stderr, "usage: astrag-core --ctl NAME --secret S --upstream PORT --listen HOST:PORT...\n");
        return 2;
    }
    prctl(PR_SET_PDEATHSIG, SIGTERM);
    prctl(PR_SET_NAME, "astrag-core");
    signal(SIGPIPE, SIG_IGN);
    signal(SIGTERM, on_term);
    signal(SIGINT, on_term);
    /* A fixed mmap threshold stops glibc from moving big stream buffers into the brk heap,
     * where freed holes fragment and are never returned (RSS grew to the high-water mark). */
    mallopt(M_MMAP_THRESHOLD, 128 * 1024);
    mallopt(M_TRIM_THRESHOLD, 1024 * 1024);
    mallopt(M_ARENA_MAX, 2);
    crc_init();
    g_now = now_s();
    g_ssl = SSL_CTX_new(TLS_client_method());
    if (!g_ssl)
        return 3;
    SSL_CTX_set_verify(g_ssl, SSL_VERIFY_NONE, NULL);
#ifdef SSL_OP_IGNORE_UNEXPECTED_EOF
    SSL_CTX_set_options(g_ssl, SSL_OP_IGNORE_UNEXPECTED_EOF);
#endif
    SSL_CTX_set_session_cache_mode(g_ssl, SSL_SESS_CACHE_CLIENT);
    g_ep = epoll_create1(EPOLL_CLOEXEC);
    if (pipe2(g_dpipe, O_CLOEXEC | O_NONBLOCK))
        return 3;
    set_nonblock(g_dpipe[0]);
    int fl = fcntl(g_dpipe[1], F_GETFL);
    fcntl(g_dpipe[1], F_SETFL, fl & ~O_NONBLOCK);
    g_dev.type = EV_DNS;
    g_dev.fd = g_dpipe[0];
    ev_watch(&g_dev, EPOLLIN);
    for (int i = 0; i < 3; i++) {
        pthread_t t;
        pthread_attr_t a;
        pthread_attr_init(&a);
        pthread_attr_setstacksize(&a, 256 * 1024);
        pthread_create(&t, &a, dns_thread, NULL);
        pthread_detach(t);
    }
    for (int i = 0; i < nl; i++) {
        char hp[128];
        snprintf(hp, sizeof hp, "%s", listens[i]);
        char *c = strrchr(hp, ':');
        if (!c)
            return 2;
        *c = 0;
        if (listener_add(hp, atoi(c + 1)))
            return 4;
    }
    int cfd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    struct sockaddr_un su;
    memset(&su, 0, sizeof su);
    su.sun_family = AF_UNIX;
    size_t cl = strlen(ctl_name);
    if (cl > sizeof su.sun_path - 2)
        return 2;
    memcpy(su.sun_path + 1, ctl_name, cl);
    if (bind(cfd, (struct sockaddr *)&su, offsetof(struct sockaddr_un, sun_path) + 1 + cl) || listen(cfd, 4)) {
        LOG("control socket: %s", strerror(errno));
        return 5;
    }
    g_ctl_lev.type = EV_CTL_LISTEN;
    g_ctl_lev.fd = cfd;
    ev_watch(&g_ctl_lev, EPOLLIN);
    LOG("started, upstream 127.0.0.1:%d", g_upstream_port);
    double next_sec = g_now + 1.0, next_trim = g_now + 10.0;
    struct epoll_event evs[256];
    while (!g_stop) {
        int n = epoll_wait(g_ep, evs, 256, 20);
        g_now = now_s();
        for (int i = 0; i < n; i++) {
            ev_t *e = evs[i].data.ptr;
            if (!e || e->dead)
                continue;
            uint32_t m = evs[i].events;
            switch (e->type) {
            case EV_LISTEN:
                on_accept(e->obj);
                break;
            case EV_FRONT:
                front_on_event(e->obj, m);
                break;
            case EV_UP:
                up_on_event(e->obj, m);
                break;
            case EV_FETCH:
                fetch_on_event(e->obj, m);
                break;
            case EV_CTL_LISTEN:
                ctl_accept();
                break;
            case EV_CTL:
                ctl_on_event(e->obj, m);
                break;
            case EV_DNS:
                dns_on_ready();
                break;
            }
        }
        g_now = now_s();
        int second = g_now >= next_sec;
        if (second)
            next_sec = g_now + 1.0;
        fetch_sweep();
        for (channel_t *ch = g_channels, *nx; ch; ch = nx) {
            nx = ch->next;
            ch_tick(ch, second);
        }
        if (second)
            front_sweep();
        if (second && g_now >= next_trim) {
            next_trim = g_now + 10.0;
            malloc_trim(0);
        }
        reap();
    }
    LOG("stopping");
    return 0;
}
