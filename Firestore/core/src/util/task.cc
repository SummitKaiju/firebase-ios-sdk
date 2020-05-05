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
namespace {

/**
 * The inverse of `std::lock_guard`: it unlocks in the constructor and locks in
 * its destructor, providing a way to safely temporarily release a lock that has
 * already been acquired in the current scope.
 */
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
 * A subclass of Task that effectively gives `std::make_shared` access to Task's
 * protected constructor.
 */
struct TaskCreator : public Task {
  template <typename... Args>
  explicit TaskCreator(Args&&... args) : Task(std::forward<Args>(args)...) {
  }
};

}  // namespace

std::shared_ptr<Task> Task::Create(Executor* executor,
                                   Executor::Operation&& operation) {
  return Create(executor, Executor::TimePoint(), Executor::kNoTag, UINT32_C(0),
                std::move(operation));
}

std::shared_ptr<Task> Task::Create(Executor* executor,
                                   Executor::TimePoint target_time,
                                   Executor::Tag tag,
                                   Executor::Id id,
                                   Executor::Operation&& operation) {
  auto task = std::make_shared<TaskCreator>(executor, target_time, tag, id,
                                            std::move(operation));
  task->self_ownership_ = task;
  return task;
}

Task::Task(Executor* executor,
           Executor::TimePoint target_time,
           Executor::Tag tag,
           Executor::Id id,
           Executor::Operation&& operation)
    : executor_(executor),
      target_time_(target_time),
      tag_(tag),
      id_(id),
      operation_(std::move(operation)) {
}

// Don't bother calling Dispose in the destructor because it just clears
// local state.
Task::~Task() = default;

void Task::Release() {
  std::lock_guard<std::mutex> lock(mutex_);
  self_ownership_.reset();
}

void Task::Execute() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (state_ == State::kInitial) {
    state_ = State::kRunning;
    executing_thread_ = std::this_thread::get_id();

    InverseLockGuard unlock(mutex_);

    operation_();
  }

  state_ = State::kDone;
  operation_ = {};

  if (executor_) {
    executor_->Complete(this);
  }
  is_complete_.notify_all();

  self_ownership_.reset();
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
    executor_ = nullptr;
    operation_ = {};
    is_complete_.notify_all();

  } else if (state_ == State::kRunning) {
    // Canceled tasks don't make any callbacks.
    executor_ = nullptr;

    // Avoid deadlocking if the current Task is triggering its own cancellation.
    auto this_thread = std::this_thread::get_id();
    if (this_thread != executing_thread_) {
      AwaitLocked(lock);
    }

  } else {
    // no-op; already kCanceled or kDone.
  }
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
