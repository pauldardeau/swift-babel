

/**
 * Perform blocking operations in background threads.
 */
class ThreadPool {

    private Int nthreads;
    private Boolean _alive;

    def ThreadPool() {
        this(2);
    }

    def ThreadPool(nthreads: Int) {
        this._alive = true;
        this.nthreads = nthreads;

        if (nthreads < 1) {
            return;
        }
    }

    def run_in_thread() {
    }

    def force_run_in_thread() {
    }

    def terminate() {
        this._alive = false;
        if (this.nthreads < 1) {
            return;
        }
    }
}

