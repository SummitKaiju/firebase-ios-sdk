/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FIRESTORE_CORE_SRC_UTIL_TASK_H_
#define FIRESTORE_CORE_SRC_UTIL_TASK_H_

#include <condition_variable>  // NOLINT(build/c++11)
#include <memory>
#include <mutex>   // NOLINT(build/c++11)
#include <thread>  // NOLINT(build/c++11)

#include "Firestore/core/src/util/executor.h"

namespace firebase {
namespace firestore {
namespace util {

/**
 * A task for an Executor to execute, either synchronously or asynchronously,
 * either immediately or after some delay.
 *
 * Nominally Tasks are owned by an Executor, but Tasks are intended to be able
 * to outlive their owner in some special cases:
 *
 *   * If the Executor implementation delegates to a system scheduling facility
 *     that does not support cancellation, the Executor can `Cancel` the task
 *     and release its ownership. When the system gets around to Executing the
 *     Task, it will be a no-op.
 *   * If the Executor is being destroyed from a Task owned by the Executor, the
 *     Task naturally has to outlive the Executor.
 *
 * To support this, Task internally keeps a `shared_ptr` to itself--an
 * intentional cycle that is broken either when the task Executes or is
 * Released.
 */
class Task : public std::enable_shared_from_this<Task> {
 public:
  /**
   * Constructs a new Task for immediate execution.
   *
   * @param executor The Executor that owns the Task.
   * @param operation The operation to perform.
   */
  static std::shared_ptr<Task> Create(Executor* executor,
                                      Executor::Operation&& operation);

  /**
   * Constructs a new Task for delayed execution.
   *
   * @param executor The Executor that owns the Task.
   * @param target_time The absolute time after which the task should execute.
   * @param tag The implementation-defined type of the task.
   * @param id The number identifying the specific instance of the task.
   * @param operation The operation to perform.
   */
  static std::shared_ptr<Task> Create(Executor* executor,
                                      Executor::TimePoint target_time,
                                      Executor::Tag tag,
                                      Executor::Id id,
                                      Executor::Operation&& operation);

 protected:
  Task(Executor* executor,
       Executor::TimePoint target_time,
       Executor::Tag tag,
       Executor::Id id,
       Executor::Operation&& operation);

 public:
  Task(const Task& other) = delete;
  Task(Task&& other) noexcept = delete;

  Task& operator=(const Task& other) = delete;
  Task& operator=(Task&& other) noexcept = delete;

  ~Task();

  /**
   * Executes the operation if the Task has not already been executed or
   * canceled. Regardless of whether or not the operation runs, releases the
   * task's ownership of itself.
   */
  void Execute();

  /**
   * Releases the task's ownership of itself without executing the task.
   */
  void Release();

  /**
   * Waits until the task has completed execution or cancellation.
   */
  void Await();

  /**
   * Cancels the task. Tasks that have not yet started running will be prevented
   * from running.
   *
   * If the task is currently executing while it is invoked, `Cancel` will await
   * the completion of the Task. This makes `Cancel` safe to call in the
   * destructor of an Executor: any currently executing tasks will extend the
   * lifetime of the Executor.
   *
   * However, if the current task is triggering its own cancellation, `Cancel`
   * will *not* wait because this would cause a deadlock. This makes it possible
   * for a Task to destroy the Executor that owns it and is compatible with
   * expectations that Task might have: after destroying the Executor it
   * obviously cannot be referenced again.
   *
   * Task guarantees that by the time `Cancel` has returned, the task will make
   * no callbacks to the owning executor. This ensures that Tasks that survive
   * past the end of the executor's life do not use after free.
   *
   * Taken together, these properties make it such that the Executor can
   * `Cancel` all pending tasks in its destructor and the right thing will
   * happen:
   *
   *   * Tasks that haven't started yet won't run.
   *   * Tasks that are currently running will extend the lifetime of the
   *     Executor.
   *   * Tasks that are destroying the Executor won't deadlock.
   */
  void Cancel();

  /**
   * Returns true if the Task is suitable for immediate execution. That is, it
   * was created without a target time.
   */
  bool is_immediate() const {
    // tag_ is immutable; no locking required
    return tag_ == Executor::kNoTag;
  }

  /**
   * Returns the target time for execution of the Task. If the task is immediate
   * this will be a zero value in the past.
   */
  Executor::TimePoint target_time() const {
    // target_time_ is immutable; no locking required.
    return target_time_;
  }

  /**
   * Returns the tag supplied in the custructor or `Executor::kNoTag`.
   */
  Executor::Tag tag() const {
    // tag_ is immutable; no locking required.
    return tag_;
  }

  Executor::Id id() const {
    // id_ is immutable; no locking required.
    return id_;
  }

  bool operator<(const Task& rhs) const;

 private:
  enum class State {
    kInitial,   // Waiting to run (or be canceled)
    kCanceled,  // Has not run and has been canceled
    kRunning,   // Now running and can no longer be canceled
    kDone,      // Has run and has finished running; cannot be canceled
  };

  void AwaitLocked(std::unique_lock<std::mutex>& lock);

  std::mutex mutex_;
  std::condition_variable is_complete_;
  State state_ = State::kInitial;

  // See class comments.
  std::shared_ptr<Task> self_ownership_;

  Executor* executor_ = nullptr;
  Executor::TimePoint target_time_;
  Executor::Tag tag_ = 0;
  Executor::Id id_ = 0;

  Executor::Operation operation_;
  std::thread::id executing_thread_;
};

/**
 * Converts a delay into an absolute TimePoint representing the current time
 * plus the delay.
 *
 * @param delay The number of milliseconds to delay.
 * @return A time representing now plus the delay.
 */
Executor::TimePoint MakeTargetTime(Executor::Milliseconds delay);

}  // namespace util
}  // namespace firestore
}  // namespace firebase

#endif  // FIRESTORE_CORE_SRC_UTIL_TASK_H_
