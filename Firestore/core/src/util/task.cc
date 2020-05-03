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

#include "Firestore/core/src/util/task.h"

#include <chrono>  // NOLINT(build/c++11)
#include <cstdint>
#include <utility>

#include "Firestore/core/src/util/hard_assert.h"

namespace firebase {
namespace firestore {
namespace util {

class ReleasingLockGuard {
 public:
  explicit ReleasingLockGuard(Task* task) : task_(task) {
    task_->mutex_.lock();
  }

  ~ReleasingLockGuard() {
    task_->ReleaseAndUnlock();
  }

 private:
  Task* task_ = nullptr;
};

class InverseLockGuard {
 public:
  explicit InverseLockGuard(std::mutex& mutex) : mutex_(mutex) {
    mutex_.unlock();
  }

  ~InverseLockGuard() {
    mutex_.lock();
  }

 private:
  std::mutex& mutex_;
};

/**
 * Returns the initial reference count for a Task based on whether or not the
 * task shares ownership with the executor that created it.
 *
 * @param executor The executor that owns the Task, or `nullptr` if the Task
 *     owns itself.
 * @return The initial reference count value.
 */
int InitialRefCount(Executor* executor) {
  return executor ? 2 : 1;
}

Task::Task(Executor* executor, Executor::Operation&& operation)
    : ref_count_(InitialRefCount(executor)),
      executor_(executor),
      target_time_(),  // immediate
      tag_(Executor::kNoTag),
      id_(UINT32_C(0)),
      operation_(std::move(operation)) {
}

Task::Task(Executor* executor,
           Executor::TimePoint target_time,
           Executor::Tag tag,
           Executor::Id id,
           Executor::Operation&& operation)
    : ref_count_(InitialRefCount(executor)),
      executor_(executor),
      target_time_(target_time),
      tag_(tag),
      id_(id),
      operation_(std::move(operation)) {
}

// Don't bother calling Dispose in the destructor because it just clears
// local state.
Task::~Task() = default;

void Task::Retain() {
  std::lock_guard<std::mutex> lock(mutex_);

  ref_count_++;
}

void Task::Release() {
  ReleasingLockGuard lock(this);
}

void Task::ReleaseAndUnlock() {
  ref_count_--;

  HARD_ASSERT(ref_count_ >= 0);

  bool should_delete = ref_count_ == 0;
  mutex_.unlock();

  if (should_delete) {
    delete this;
  }
}

void Task::Execute() {
  {
    ReleasingLockGuard lock(this);

    if (state_ == State::kInitial) {
      state_ = State::kRunning;

      InverseLockGuard unlock(mutex_);

      // Mark the operation complete before executing it to avoid a race with
      // any task that attempts to acquire a lock on the Executor as a
      // consequence of the operation executing.
      if (executor_) {
        executor_->Complete(this);
      }
      operation_();
    }

    state_ = State::kDone;
    operation_ = {};
    is_complete_.notify_all();
  }
}

void Task::Await() {
  std::unique_lock<std::mutex> lock(mutex_);
  AwaitLocked(lock);
}

void Task::AwaitLocked(std::unique_lock<std::mutex>& lock) {
  is_complete_.wait(lock, [this] {
    return state_ == State::kCanceled || state_ == State::kDone;
  });
}

void Task::Cancel() {
  std::unique_lock<std::mutex> lock(mutex_);

  if (state_ == State::kInitial) {
    state_ = State::kCanceled;
    operation_ = {};
    is_complete_.notify_all();

  } else if (state_ == State::kRunning) {
    AwaitLocked(lock);

  } else {
    // no-op; already kCanceled or kDone.
  }

  executor_ = nullptr;
}

bool Task::operator<(const Task& rhs) const {
  // target_time_ and id_ are immutable after assignment; no lock required.

  // Order by target time, then by the order in which entries were created.
  if (target_time_ < rhs.target_time_) {
    return true;
  }
  if (target_time_ > rhs.target_time_) {
    return false;
  }

  return id_ < rhs.id_;
}

Executor::TimePoint MakeTargetTime(Executor::Milliseconds delay) {
  return std::chrono::time_point_cast<Executor::Milliseconds>(
             Executor::Clock::now()) +
         delay;
}

}  // namespace util
}  // namespace firestore
}  // namespace firebase
