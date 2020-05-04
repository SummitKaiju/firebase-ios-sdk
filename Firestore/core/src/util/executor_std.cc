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

#include "Firestore/core/src/util/executor_std.h"

#include <future>  // NOLINT(build/c++11)
#include <memory>
#include <sstream>

#include "Firestore/core/src/util/task.h"
#include "absl/memory/memory.h"

namespace firebase {
namespace firestore {
namespace util {

namespace {

// As a convention, assign the epoch time to all operations scheduled for
// immediate execution. Note that it means that an immediate operation is
// always scheduled before any delayed operation, even in the corner case when
// the immediate operation was scheduled after a delayed operation was due
// (but hasn't yet run).
Executor::TimePoint Immediate() {
  return Executor::TimePoint{};
}

// The minimum time point, used to enqueue things at the absolute front of the
// schedule.
Executor::TimePoint Min() {
  return Executor::TimePoint{Executor::TimePoint::duration::min()};
}

// The only guarantee is that different `thread_id`s will produce different
// values.
std::string ThreadIdToString(const std::thread::id thread_id) {
  std::ostringstream stream;
  stream << thread_id;
  return stream.str();
}

}  // namespace

// MARK: - ExecutorStd

ExecutorStd::ExecutorStd(int threads) {
  HARD_ASSERT(threads > 0);

  // Somewhat counter-intuitively, constructor of `std::atomic` assigns the
  // value non-atomically, so the atomic initialization must be provided here,
  // before the worker thread is started.
  // See [this thread](https://stackoverflow.com/questions/25609858) for context
  // on the constructor.
  current_id_ = 0;

  for (int i = 0; i < threads; ++i) {
    worker_thread_pool_.emplace_back(&ExecutorStd::PollingThread, this);
  }
}

ExecutorStd::~ExecutorStd() {
  schedule_.Clear();

  // Enqueue one Task with the kShutdownTag for each worker. Workers will finish
  // whatever task they're currently working on, execute this task, and then
  // quit.
  //
  // Note that this destructor may be running on a thread managed by this
  // Executor. This means that these tasks cannot be Awaited, though we do so
  // indirectly by joining threads if possible. On the thread currently running
  // this destructor, the kShutdownTag Task will execute after the destructor
  // completes.
  for (size_t i = 0; i < worker_thread_pool_.size(); ++i) {
    PushOnSchedule(Min(), kShutdownTag, [] {});
  }

  for (std::thread& thread : worker_thread_pool_) {
    // If the current thread is running this destructor, we can't join the
    // thread. Instead detach it and rely on PollingThread to exit cleanly.
    if (std::this_thread::get_id() == thread.get_id()) {
      thread.detach();
    } else {
      thread.join();
    }
  }
}

void ExecutorStd::Execute(Operation&& operation) {
  PushOnSchedule(Immediate(), kNoTag, std::move(operation));
}

DelayedOperation ExecutorStd::Schedule(const Milliseconds delay,
                                       Tag tag,
                                       Operation&& operation) {
  // While negative delay can be interpreted as a request for immediate
  // execution, supporting it would provide a hacky way to modify FIFO ordering
  // of immediate operations.
  HARD_ASSERT(delay.count() >= 0, "Schedule: delay cannot be negative");

  const auto target_time = MakeTargetTime(delay);
  const auto id = PushOnSchedule(target_time, tag, std::move(operation));
  return DelayedOperation(this, id);
}

void ExecutorStd::Complete(Task*) {
  // No-op in this implementation
}

void ExecutorStd::Cancel(const Id operation_id) {
  auto removed = schedule_.RemoveIf(
      [operation_id](Task* t) { return t->id() == operation_id; });
  if (removed) {
    removed->Release();
  }
}

ExecutorStd::Id ExecutorStd::PushOnSchedule(const TimePoint when,
                                            const Tag tag,
                                            Operation&& operation) {
  // Note: operations scheduled for immediate execution don't actually need an
  // id. This could be tweaked to reuse the same id for all such operations.
  const auto id = NextId();
  schedule_.Push(new Task(nullptr, when, tag, id, std::move(operation)));
  return id;
}

void ExecutorStd::PollingThread() {
  for (;;) {
    Task* task = schedule_.PopBlocking();
    bool shutdown_requested = task->tag() == kShutdownTag;

    task->Execute();
    if (shutdown_requested) {
      break;
    }
  }
}

ExecutorStd::Id ExecutorStd::NextId() {
  // The wrap around after ~4 billion operations is explicitly ignored. Even if
  // an instance of `ExecutorStd` runs long enough to get `current_id_` to
  // overflow, it's extremely unlikely that any object still holds a reference
  // that is old enough to cause a conflict.
  return current_id_++;
}

bool ExecutorStd::IsCurrentExecutor() const {
  auto current_id = std::this_thread::get_id();
  for (const std::thread& thread : worker_thread_pool_) {
    if (thread.get_id() == current_id) {
      return true;
    }
  }
  return false;
}

std::string ExecutorStd::CurrentExecutorName() const {
  if (IsCurrentExecutor()) {
    return Name();
  } else {
    return ThreadIdToString(std::this_thread::get_id());
  }
}

std::string ExecutorStd::Name() const {
  return ThreadIdToString(worker_thread_pool_.front().get_id());
}

void ExecutorStd::ExecuteBlocking(Operation&& operation) {
  std::promise<void> signal_finished;
  Execute([&] {
    operation();
    signal_finished.set_value();
  });
  signal_finished.get_future().wait();
}

bool ExecutorStd::IsScheduled(const Tag tag) const {
  return schedule_.Contains([&tag](Task* t) { return t->tag() == tag; });
}

bool ExecutorStd::IsTaskScheduled(const Id id) const {
  return schedule_.Contains([&id](Task* t) { return t->id() == id; });
}

Task* ExecutorStd::PopFromSchedule() {
  return schedule_.RemoveIf([](Task* t) { return !t->is_immediate(); });
}

// MARK: - Executor

// Only defined on non-Apple platforms. On Apple platforms, see the alternative
// definition in executor_libdispatch.mm.
#if !__APPLE__

std::unique_ptr<Executor> Executor::CreateSerial(const char*) {
  return absl::make_unique<ExecutorStd>(/*threads=*/1);
}

std::unique_ptr<Executor> Executor::CreateConcurrent(const char*, int threads) {
  return absl::make_unique<ExecutorStd>(threads);
}

#endif  // !__APPLE__

}  // namespace util
}  // namespace firestore
}  // namespace firebase
