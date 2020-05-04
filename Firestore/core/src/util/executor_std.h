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

#ifndef FIRESTORE_CORE_SRC_UTIL_EXECUTOR_STD_H_
#define FIRESTORE_CORE_SRC_UTIL_EXECUTOR_STD_H_

#include <algorithm>
#include <atomic>
#include <condition_variable>  // NOLINT(build/c++11)
#include <deque>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <string>
#include <thread>  // NOLINT(build/c++11)
#include <utility>
#include <vector>

#include "Firestore/core/src/util/executor.h"
#include "Firestore/core/src/util/hard_assert.h"
#include "Firestore/core/src/util/schedule.h"
#include "absl/types/optional.h"

namespace firebase {
namespace firestore {
namespace util {

class Task;

// A serial queue that executes provided operations on a dedicated background
// thread, using C++11 standard library functionality.
class ExecutorStd : public Executor {
 public:
  static constexpr Tag kShutdownTag = -2;

  explicit ExecutorStd(int threads);
  ~ExecutorStd();

  void Execute(Operation&& operation) override;
  void ExecuteBlocking(Operation&& operation) override;

  DelayedOperation Schedule(Milliseconds delay,
                            Tag tag,
                            Operation&& operation) override;

  bool IsCurrentExecutor() const override;
  std::string CurrentExecutorName() const override;
  std::string Name() const override;

  bool IsScheduled(Tag tag) const override;
  bool IsTaskScheduled(Id id) const override;
  Task* PopFromSchedule() override;

 private:
  Id PushOnSchedule(TimePoint when, Tag tag, Operation&& operation);

  void Complete(Task* task) override;
  void Cancel(Id operation_id) override;

  void PollingThread();
  Id NextId();

  // Operations scheduled for immediate execution are also put on the schedule
  // (with due time set to `Immediate`).
  class Schedule schedule_;

  std::vector<std::thread> worker_thread_pool_;

  std::atomic<Id> current_id_{0};
};

}  // namespace util
}  // namespace firestore
}  // namespace firebase

#endif  // FIRESTORE_CORE_SRC_UTIL_EXECUTOR_STD_H_
