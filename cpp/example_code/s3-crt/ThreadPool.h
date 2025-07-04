#pragma once

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <iostream>

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
                            return stop.load() || !tasks.empty();
                        });

                        if (stop.load() && tasks.empty())
                            return;

                        task = std::move(tasks.front());
                        tasks.pop();
                        spaceAvailable.notify_one();
                    }

                    if (task) {
                        try {
                            task();
                        } catch (const std::exception& ex) {
                            std::cerr << "[ThreadPool] Task exception: " << ex.what() << std::endl;
                        } catch (...) {
                            std::cerr << "[ThreadPool] Unknown task exception\n";
                        }
                    }
                }
            });
        }
    }

    template <class F>
    bool enqueue(F&& f) {
        std::unique_lock<std::mutex> lock(queueMutex);
        spaceAvailable.wait(lock, [this]() {
            return tasks.size() < maxQueueSize || stop.load();
        });

        if (stop.load())
            return false;

        tasks.push(std::forward<F>(f));
        condition.notify_one();
        return true;
    }

    size_t getQueueSize() {
        std::lock_guard<std::mutex> lock(queueMutex);
        return tasks.size();
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            stop.store(true);
        }

        condition.notify_all();
        spaceAvailable.notify_all();

        for (auto& worker : workers) {
            if (worker.joinable())
                worker.join();
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

    std::atomic<bool> stop;
    size_t maxQueueSize;
};

