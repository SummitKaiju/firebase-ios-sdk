/*
 * Copyright 2018 Google LLC
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
#include <mutex>  // NOLINT(build/c++11)

#include "Firestore/core/src/util/executor.h"

namespace firebase {
namespace firestore {
namespace util {

/**
 * A task for an Executor to execute, either synchronously or asynchronously,
 * either immediately or after some delay.
 */
class Task {
 public:
  /**
   * Constructs a new Task for immediate execution.
   *
   * @param executor The Executor that owns the Task.
   * @param operation The operation to perform.
   */
  Task(Executor* executor, Executor::Operation&& operation);

  /**
   * Constructs a new Task for delayed execution.
   *
   * @param executor The Executor that owns the Task.
   * @param target_time The absolute time after which the task should execute.
   * @param tag The implementation-defined type of the task.
   * @param id The number identifying the specific instance of the task.
   * @param operation The operation to perform.
   */
  Task(Executor* executor,
       Executor::TimePoint target_time,
       Executor::Tag tag,
       Executor::Id id,
       Executor::Operation&& operation);

  Task(const Task& other) = delete;
  Task(Task&& other) noexcept = delete;

  Task& operator=(const Task& other) = delete;
  Task& operator=(Task&& other) noexcept = delete;

  ~Task();

  /** Increment the reference count. */
  void Retain();

  /**
   * Decrement the reference count. The object is deleted when the reference
   * count goes to zero.
   */
  void Release();

  /**
   * Executes the operation if the Task has not already been executed or
   * canceled. Regardless of whether or not the operation runs, decrements the
   * reference count.
   */
  void Execute();

  /**
   * Waits until the task has completed.
   */
  void Await();

  /**
   * Cancels the task, marking it done, and otherwise preventing it from
   * interacting with the rest of the system.
   */
  void Cancel();

  bool is_immediate() const {
    // tag_ is immutable; no locking required
    return tag_ == Executor::kNoTag;
  }

  Executor::Tag tag() const {
    // tag_ is immutable; no locking required
    return tag_;
  }

  Executor::Id id() const {
    // id_ is immutable; no locking required
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

  friend class ReleasingLockGuard;

  void AwaitLocked(std::unique_lock<std::mutex>& lock);
  void ReleaseAndUnlock();

  std::mutex mutex_;
  std::condition_variable is_complete_;
  State state_ = State::kInitial;

  int ref_count_ = 0;

  Executor* executor_ = nullptr;
  Executor::TimePoint target_time_;
  Executor::Tag tag_ = 0;
  Executor::Id id_ = 0;

  Executor::Operation operation_;
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
