module ThreadPool;


/**
 * Perform blocking operations in background threads.
 */
class ThreadPool {

private:
    int nthreads;
    bool _alive;

public:
    this(int nthreads=2) {
        this._alive = true;
        this.nthreads = nthreads;

        if (nthreads < 1) {
            return;
        }
    }

    void run_in_thread() {
    }

    void force_run_in_thread() {
    }

    void terminate() {
        this._alive = false;
        if (this.nthreads < 1) {
            return;
        }
    }
}

