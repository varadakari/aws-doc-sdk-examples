#pragma once

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

class ThreadPool {
public:
    ThreadPool(size_t numThreads, size_t maxQueueSize)
        : stop(false), maxQueueSize(maxQueueSize) {
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this]() {
                while (true) {
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(queueMutex);
                        condition.wait(lock, [this]() {
                            return stop || !tasks.empty();
                        });

                        if (stop && tasks.empty())
                            return;

                        task = std::move(tasks.front());
                        tasks.pop();
                        spaceAvailable.notify_one(); // notify producer
                    }

                    task();
                }
            });
        }
    }

    template <class F>
    void enqueue(F&& f) {
        std::unique_lock<std::mutex> lock(queueMutex);
        spaceAvailable.wait(lock, [this]() {
            return tasks.size() < maxQueueSize || stop;
        });

        if (stop)
            return;

        tasks.push(std::forward<F>(f));
        condition.notify_one();
    }

    size_t getQueueSize() {
        std::lock_guard<std::mutex> lock(queueMutex);
        return tasks.size();
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            stop = true;
        }

        condition.notify_all();
        spaceAvailable.notify_all();

        for (size_t i = 0; i < workers.size(); ++i) {
            if (workers[i].joinable())
                workers[i].join();
        }
    }

    ~ThreadPool() {
        shutdown();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queueMutex;
    std::condition_variable condition;
    std::condition_variable spaceAvailable;

    bool stop;
    size_t maxQueueSize;
};

