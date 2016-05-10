

/**
 * Perform blocking operations in background threads.
 */
public class ThreadPool {

    private int nthreads;
    private boolean _alive;

    public ThreadPool() {
        this(2);
    }

    public ThreadPool(int nthreads) {
        this._alive = true;
        this.nthreads = nthreads;

        if (nthreads < 1) {
            return;
        }
    }

    public void run_in_thread() {
    }

    public void force_run_in_thread() {
    }

    public void terminate() {
        this._alive = false;
        if (this.nthreads < 1) {
            return;
        }
    }
}

