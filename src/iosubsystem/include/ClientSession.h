#ifndef CLIENT_SESSION_H
#define CLIENT_SESSION_H

#include <vector>
#include <map>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <atomic>
#include <string>
#include <cassert>

namespace GaneshaDB {
template <typename T>
class TableSession {
public:
	using ID = int64_t;

	enum class State {
		NEW,
		READY,
	};

	TableSession(ID id, const std::string& uuid) : id_(id), uuid_(uuid),
			state_(State::NEW) {
	}

	void ready(std::unique_ptr<T> object) {
		object_ = std::move(object);
		state_  = State::READY;
		refcnt_++;
	}

	State getState() const {
		return state_;
	}

	ID getIDAndRefer() {
		refcnt_++;
		return id_;
	}

	ID getID() const {
		return id_;
	}

	const std::string& getUUID() const {
		return uuid_;
	}

	template <typename Lambda>
	void put(Lambda&& cb) {
		auto c = --refcnt_;
		if (c == 0) {
			cb(id_, uuid_, std::move(object_), c);
		} else {
			cb(id_, uuid_, nullptr, c);
		}
	}

private:
	std::unique_ptr<T>    object_={nullptr};
	ID                    id_;
	std::string           uuid_;
	State                 state_;
	std::atomic<uint32_t> refcnt_={0};
};

template <typename T>
class ClientSession {
public:
	ClientSession() {

	}

	~ClientSession() {
		/* dump all unclosed sessions */
	}

	ClientSession(const ClientSession& rhs) = delete;
	bool operator==(const ClientSession& rhs) = delete;

	/*
	 * reserveSessionID
	 *
	 * - generates a new TableSession::ID and maps it with UUID.
	 */

	typename TableSession<T>::ID reserveSessionID(const std::string& uuid,
				typename TableSession<T>::State& state) {
		std::unique_lock<std::mutex> lock(mutex_);
		typename TableSession<T>::ID id = -1;

		while (id == -1) {
			auto it = uuidToObject_.find(uuid);
			if (it == uuidToObject_.end()) {
				id                  = ++curID_;
				auto sp             = std::make_unique<TableSession<T>>(id, uuid);
				state               = sp->getState();
				uuidToObject_[uuid] = std::move(sp);
				assert(state == TableSession<T>::State::NEW);
			} else {
				auto sp = it->second.get();
				auto st = sp->getState();
				switch (st) {
				case TableSession<T>::State::NEW:
					cv_.wait(lock);
					id = -1;
					break;
				case TableSession<T>::State::READY:
					id = sp->getIDAndRefer();
					assert(id != -1);
					state = st;
					break;
				}
			}
		}
		assert(id != -1);
		nliveSessions_++;
		return id;
	}

	int setSessionReady(typename TableSession<T>::ID id, const std::string& uuid,
				std::unique_ptr<T> table) {
		std::lock_guard<std::mutex> lock(mutex_);

		auto idit = idToObject_.find(id);
		assert(idit == idToObject_.end());
		(void) idit;

		auto it         = uuidToObject_.find(uuid);
		auto sp         = it->second.get();
		if (sp->getUUID() != uuid || sp->getID() != id) {
			/* miss-match */
			return -1;
		}

		idToObject_[id] = table.get();
		sp->ready(std::move(table));

		cv_.notify_all();
		return 0;
	}

	void getSession(typename TableSession<T>::ID id, T** tablepp) {
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = idToObject_.find(id);
		if (it == idToObject_.end()) {
			*tablepp = nullptr;
			return;
		}
		*tablepp = it->second;
	}

	template <typename Lambda>
	int putSession(typename TableSession<T>::ID id, const std::string& uuid,
				Lambda&& cb) {
		std::unique_lock<std::mutex> lock(mutex_);

		auto it = uuidToObject_.find(uuid);
		assert (it != uuidToObject_.end());
		auto sp = it->second.get();
		if (sp->getUUID() != uuid || sp->getID() != id) {
			/* miss-match */
			return -1;
		}
		nliveSessions_--;
		sp->put([this, &lock, &uuid, id, &cb] (typename TableSession<T>::ID id2,
					const std::string& uuid2, std::unique_ptr<T> table,
					uint32_t refCnt) {
			if (refCnt != 0) {
				cb(nullptr, false);
			} else {
				assert(uuid == uuid2 && id == id2);

				auto idit = this->idToObject_.find(id);
				assert(idit != this->idToObject_.end());
				this->idToObject_.erase(idit);

				auto it      = this->uuidToObject_.find(uuid);
				auto session = std::move(it->second);
				this->uuidToObject_.erase(it);
				lock.unlock();

				cb(std::move(table), true);
			}
		});
		return 0;
	}

	uint64_t getNLiveSessions() const {
		return nliveSessions_;
	}

private:
	std::mutex                                              mutex_;
	std::condition_variable                                 cv_;
	std::map<std::string, std::unique_ptr<TableSession<T>>> uuidToObject_;
	std::map<typename TableSession<T>::ID, T*>              idToObject_;
	typename TableSession<T>::ID                            curID_ = {0};
	std::atomic<uint64_t>                                   nliveSessions_{0};
};
}; // namespace

#endif
