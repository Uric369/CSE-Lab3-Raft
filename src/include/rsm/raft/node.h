#pragma once

#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>

#include "block/manager.h"
#include "common/logger.h"
#include "common/util.h"
#include "fmt/core.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"
namespace debug {
    template <typename Command>
    std::string entries_to_str(std::vector<chfs::Entry<Command>> entries) {
        std::string entries_str = "[";
        for (const chfs::Entry<Command> &entry : entries) {
            entries_str += entry.to_string() + ",";
        }
        if (entries_str.size() != 1) {
            entries_str.pop_back();
        }
        entries_str += "]";
        return entries_str;
    }
};  // namespace debug
namespace chfs {

    class Timer {
    public:
        Timer(int random, int base) : random(random), base(base) { interval = generator.rand(0, random) + base; };
        void reset() {
            interval = generator.rand(0, random) + base;
            start_time = std::chrono::steady_clock::now();
            receive_heartbeat = false;
        }
        auto get_interval() const { return std::chrono::milliseconds(interval); }
        void start() {
            start_time = std::chrono::steady_clock::now();
            state = true;
        }
        void stop() { state = false; }
        void receive() { receive_heartbeat = true; }
        bool check_receive() { return receive_heartbeat.load(); }
        bool timeout() {
            if (!state) {
                return false;
            }
            auto curr_time = std::chrono::steady_clock::now();
            if (const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - start_time).count();
                    duration > interval) {
                reset();
                return true;
            }
            return false;
        }

    private:
        int random;
        int base;
        int interval;
        std::chrono::steady_clock::time_point start_time;
        RandomNumberGenerator generator{};
        std::atomic<bool> receive_heartbeat{false};
        std::atomic<bool> state{false};
    };
    struct RaftNodePersist {
        int term;
        int voteFor;
        int block_num;
    };
    enum class RaftRole { Follower, Candidate, Leader };
    struct RaftNodeConfig {
        int node_id;
        uint16_t port;
        std::string ip_address;
    };

    template <typename StateMachine, typename Command>
    class RaftNode {
#define RAFT_LOG(fmt, args...)                                                                                     \
  do {                                                                                                             \
    long now =                                                                                                     \
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
            .count();                                                                                              \
    char buf[512];                                                                                                 \
    snprintf(buf, 512, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now % 100000, __FILE_NAME__, __LINE__,   \
             my_id, current_term, role, ##args);                                                                   \
    debug_thread_pool->enqueue([=]() { std::cerr << buf; });                                                       \
  } while (0);

    public:
        RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
        ~RaftNode();

        /* interfaces for test */
        void set_network(std::map<int, bool> &network_availability);
        void set_reliable(bool flag);
        int get_list_state_log_num();
        int rpc_count();
        std::vector<u8> get_snapshot_direct();

    private:
        /*
         * Start the raft node.
         * Please make sure all of the rpc request handlers have been registered before this method.
         */
        auto start() -> int;

        /*
         * Stop the raft node.
         */
        auto stop() -> int;

        /* Returns whether this node is the leader, you should also return the current term. */
        auto is_leader() -> std::tuple<bool, int>;

        /* Checks whether the node is stopped */
        auto is_stopped() -> bool;

        /*
         * Send a new command to the raft nodes.
         * The returned tuple of the method contains three values:
         * 1. bool:  True if this raft node is the leader that successfully appends the log,
         *      false If this node is not the leader.
         * 2. int: Current term.
         * 3. int: Log index.
         */
        auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

        /* Save a snapshot of the state machine and compact the log. */
        auto save_snapshot() -> bool;

        /* Get a snapshot of the state machine */
        auto get_snapshot() -> std::vector<u8>;

        /* Internal RPC handlers */
        auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
        auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
        auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

        /* RPC helpers */
        void send_request_vote(int target, RequestVoteArgs arg);
        void handle_request_vote_reply(int target, RequestVoteArgs arg, RequestVoteReply reply);

        void send_append_entries(int target, AppendEntriesArgs<Command> arg);
        void handle_append_entries_reply(int target, AppendEntriesArgs<Command> arg, AppendEntriesReply reply);

        void send_install_snapshot(int target, InstallSnapshotArgs arg);
        void handle_install_snapshot_reply(int target, InstallSnapshotArgs arg, InstallSnapshotReply reply);

        /* background workers */
        void run_background_ping();
        void run_background_election();
        void run_background_commit();
        void run_background_apply();

        /* Data structures */
        bool network_stat; /* for test */

        std::mutex mtx;         /* A big lock to protect the whole data structure. */
        std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
        std::unique_ptr<ThreadPool> thread_pool;
        std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
        std::unique_ptr<StateMachine> state;           /*  The state machine that applies the raft log, e.g. a kv store. */

        std::unique_ptr<RpcServer> rpc_server;                     /* RPC server to recieve and handle the RPC requests. */
        std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map; /* RPC clients of all raft nodes including this node. */
        std::vector<RaftNodeConfig> node_configs;                  /* Configuration for all nodes */
        int my_id;                                                 /* The index of this node in rpc_clients, start from 0. */

        std::atomic_bool stopped;

        RaftRole role;
        int current_term;
        int leaderId;

        std::unique_ptr<std::thread> background_election;
        std::unique_ptr<std::thread> background_ping;
        std::unique_ptr<std::thread> background_commit;
        std::unique_ptr<std::thread> background_apply;

        /* Lab3: Your code here */
        int commit_index;
        int voteFor;
        std::atomic<int> granted_vote;
        std::map<int, int> next_index;
        std::map<int, int> match_index;

        std::vector<int> peer;
        std::unique_ptr<ThreadPool> debug_thread_pool;
        Timer vote_timer;
        std::shared_ptr<BlockManager> bm;
        int lastIncludeIndex{0};
        int last_include_term{0};
        int state_size{1};

        void become_leader();
        void become_follower(int term, int id_leader);
        void become_candidate();
        void persist();
        void recover();
    };

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs)
            : network_stat(true),
              node_configs(configs),
              my_id(node_id),
              stopped(true),
              role(RaftRole::Follower),
              current_term(0),
              leaderId(-1),
              commit_index(0),
              voteFor(-1),
              granted_vote(0),
              vote_timer(50, 500) {
        auto my_config = node_configs[my_id];

        /* launch RPC server */
        rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

        /* Register the RPCs. */
        rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
        rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
        rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
        rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
        rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                         [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
        rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
        rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

        rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
        rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
        rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

        /* Lab3: Your code here */
        thread_pool = std::make_unique<ThreadPool>(16);
        debug_thread_pool = std::make_unique<ThreadPool>(16);
        auto log_filename = fmt::format("/tmp/raft_log/{}.log", my_id);
        bm = std::make_shared<BlockManager>(log_filename);
        log_storage = std::make_unique<RaftLog<Command>>(bm);
        for (const auto &node : node_configs) {
            if (node.node_id == my_id) {
                continue;
            }
            peer.emplace_back(node.node_id);
        }

        recover();
        for (const auto &node : peer) {
            next_index[node] = log_storage->Size();
            match_index[node] = 0;
        }
        state = std::make_unique<StateMachine>();

        rpc_server->run(true, configs.size());
    }

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::~RaftNode() {
        stop();

        thread_pool.reset();
        rpc_server.reset();
        state.reset();
        log_storage.reset();
    }

/******************************************************************

                        RPC Interfaces

*******************************************************************/

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::start() -> int {
        /* Lab3: Your code here */
        for (const auto &node : node_configs) {
            rpc_clients_map[node.node_id] = std::make_unique<RpcClient>(node.ip_address, node.port, true);
        }
        stopped = false;
        role = RaftRole::Follower;
        vote_timer.start();
        background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
        background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
        background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
        background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::stop() -> int {
        /* Lab3: Your code here */
        stopped = true;
        background_election->join();
        background_ping->join();
        background_commit->join();
        background_apply->join();
        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
        // Return a tuple directly using aggregate initialization
        return {this->role == RaftRole::Leader, this->current_term};
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
        return stopped.load();
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size)
    -> std::tuple<bool, int, int> {
        /* Lab3: Your code here */
        std::scoped_lock<std::mutex> lock(mtx);
        // Get the index before potentially appending to the log.
        int log_index = log_storage->Size() - 1;

        if (role != RaftRole::Leader) {
            return {false, current_term, log_index};
        }
        Command cmd;
        cmd.deserialize(cmd_data, cmd_size);
        log_storage->Append({current_term, cmd});
        return {true, current_term, log_storage->Size() - 1};
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
        /* Lab3: Your code here */
        lastIncludeIndex = log_storage->Size() - 1;
        last_include_term = log_storage->Back().term;
        std::vector<u8> snapshot = get_snapshot();
        log_storage->SaveSnapshot(my_id, lastIncludeIndex, last_include_term, 0, snapshot, true);
        for (const auto &node : peer) {
            next_index[node] = log_storage->Size();
            match_index[node] = next_index[node] - 1;
        }
        return true;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
        return log_storage->GetSnapshot();
    }

/******************************************************************

                         Internal RPC Related

*******************************************************************/

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply {
        /* Lab3: Your code here */
        auto term = args.term;
        auto lastLogIndex = log_storage->Size() - 1;
        auto lastLogTerm = log_storage->At(lastLogIndex).term;
        bool is_term_stale = term < current_term;
        bool already_voteFor_candidate = voteFor == args.candidateId;
        bool has_voted = voteFor != -1 && voteFor != my_id;
        bool log_is_up_to_date =
                lastLogTerm > args.lastLogTerm ||
                (lastLogTerm == args.lastLogTerm && lastLogIndex > args.lastLogIndex);

        if (is_term_stale) {
            return {current_term, false};
        }
        if (already_voteFor_candidate) {
            return {current_term, true};
        }
        if (has_voted) {
            return {current_term, false};
        }
        if (log_is_up_to_date) {
            return {current_term, false};
        }
        if(voteFor == my_id) granted_vote--;
        this->role = RaftRole::Follower;
        vote_timer.start();
        this->leaderId = -1;
        this->voteFor = -1;
        this->granted_vote = 0;
        persist();
        return {current_term, true};
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                                                    const RequestVoteReply reply) {
        /* Lab3: Your code here */
//        if (reply.term > current_term) {
//            this->current_term = reply.term;
//            if (!reply.voteGranted) {
//                this->role = RaftRole::Follower;
//                vote_timer.start();
//                this->voteFor = -1;
//                this->current_term = reply.term;
//                this->leaderId = -1;
//                this->granted_vote = 0;
//                persist();
//                this->voteFor = target;
//            }
//        }
//
//        if (reply.voteGranted && role == RaftRole::Candidate) {
//            current_term = std::max(current_term, reply.term);
//            granted_vote++;
//            if (granted_vote >= (int)node_configs.size() / 2 + 1) {
//                this->role = RaftRole::Leader;
//                this->leaderId = my_id;
//                this->granted_vote = 0;
//                this->voteFor = -1;
//                vote_timer.stop();
//                persist();
//            }
//        }

        if (reply.term > current_term) {
            current_term = reply.term;
        }

        if (!reply.voteGranted && reply.term > current_term) {
            become_follower(reply.term, -1);
            voteFor = target;
        } else if (reply.voteGranted && role == RaftRole::Candidate) {
            current_term = std::max(current_term, reply.term);
            granted_vote++;
            auto half_node = (int)node_configs.size() / 2 + 1;
            RAFT_LOG("grant:%d from%d half:%d ", (int)granted_vote, target, half_node)
            if (granted_vote >= half_node) {
                become_leader();
            }
        }
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
        /* Lab3: Your code here */
        auto arg = transform_rpc_append_entries_args<Command>(rpc_arg);
        if (arg.term < current_term) {
            return {current_term, false};
        }

        // If we receive a heartbeat or append entries with an equal or higher term, we know who the leader is
        if (arg.term >= current_term) {
            voteFor = -1;
            become_follower(arg.term, arg.leaderId);
            leaderId = arg.leaderId;
            vote_timer.receive();
        }

        // If this is just a heartbeat, then we do not need to process the log entries
        if (arg.heartBeat) {
            return {current_term, true};
        }

        if (arg.lastIncludeIndex != 0) {
            lastIncludeIndex = arg.lastIncludeIndex;
            log_storage->EraseAllAfterIndex(arg.prevLogIndex + 1);
            for (const auto& entry : arg.entries) {
                log_storage->Insert(arg.prevLogIndex + 1, entry);
                ++arg.prevLogIndex; // Increment index for each inserted entry
            }
            commit_index = std::min(log_storage->Size() - 1, arg.leaderCommit);
            persist();
            return {current_term, true};
        }
        else if (arg.prevLogIndex > 0 && (arg.prevLogIndex > log_storage->Size() - 1 ||
        log_storage->At(arg.prevLogIndex).term != arg.prevLogTerm)) {
            // If log consistency check fails
            return {current_term, false};
        }

        // Apply the log entries
        log_storage->EraseAllAfterIndex(arg.prevLogIndex + 1);
        for (const auto& entry : arg.entries) {
            log_storage->Insert(arg.prevLogIndex + 1, entry);
            ++arg.prevLogIndex; // Increment index for each inserted entry
        }

        // Update the commit index and persist state
        commit_index = std::min(log_storage->Size() - 1, arg.leaderCommit);
        persist();

        // Return success
        return {current_term, true};
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg,
                                                                      const AppendEntriesReply reply) {
        if (role != RaftRole::Leader) {
            return;
        }

        if (reply.term > current_term) {
            current_term = reply.term;
            role = RaftRole::Follower;
            voteFor = -1;
            return;
        }

        if (!reply.success) {
            next_index[target] = std::max(1, next_index[target] - 1);
        } else {
            if (!arg.entries.empty()) {
                match_index[target] = arg.prevLogIndex + arg.entries.size();
                next_index[target] = match_index[target] + 1;

                int new_commit_index = commit_index;
                // Check for any new log entries that can be committed.
                for (int N = commit_index + 1; N <= match_index[target]; ++N) {
                    int count = 1; // Count includes self.
                    for (const auto& [node_id, index] : match_index) {
                        if (index >= N) {
                            count++;
                        }
                    }
                    // Check if a majority has this entry and it's from the current term.
                    if (count > node_configs.size() / 2 && log_storage->At(N).term == current_term) {
                        new_commit_index = N;
                    }
                }

                if (new_commit_index > commit_index) {
                    commit_index = new_commit_index;
                    // Apply newly committed entries to the state machine.
                    // This could be another function to apply entries up to commit_index.
                }
            }
        }
    }


    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply {
        /* Lab3: Your code here */
        if (current_term > args.term) {
            return {current_term};
        }
        //  auto filename = log_storage->SaveSnapshot(my_id, args.last_included_index, args.last_included_term, args.offset,
        //                                            args.data, args.done);
        //  if (filename == "") {
        //    return {current_term};
        //  }
        //  std::ifstream fs(filename, std::ios::binary | std::ios::in);
        //  std::string str((std::istreambuf_iterator<char>(fs)), std::istreambuf_iterator<char>());
        //  std::vector<uint8_t> data{str.begin(), str.end()};
        //  state->apply_snapshot(data);
        return {current_term};
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                                                        const InstallSnapshotReply reply) {
        /* Lab3: Your code here */
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr ||
            rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            return;
        }
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
        clients_lock.unlock();
        if (res.is_ok()) {
            handle_request_vote_reply(target_id, arg, res.unwrap()->template as<RequestVoteReply>());
        } else {
            // RPC fails
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr ||
            rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            return;
        }

        RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
        clients_lock.unlock();
        if (res.is_ok()) {
            handle_append_entries_reply(target_id, arg, res.unwrap()->template as<AppendEntriesReply>());
        } else {
            // RPC fails
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr ||
            rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            return;
        }

        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
        clients_lock.unlock();
        if (res.is_ok()) {
            handle_install_snapshot_reply(target_id, arg, res.unwrap()->template as<InstallSnapshotReply>());
        } else {
            // RPC fails
        }
    }

/******************************************************************

                        Background Workers

*******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_election() {
        while (!is_stopped()) {
                /* Lab3: Your code here */
                std::this_thread::sleep_for(vote_timer.get_interval());
                if (role == RaftRole::Leader || !rpc_clients_map[my_id]) {
                    continue;
                }

                auto receive = vote_timer.check_receive();
                auto timeout = vote_timer.timeout();
                if (role == RaftRole::Follower && timeout && !receive) become_candidate();

                if (role == RaftRole::Candidate) {
                    current_term++;
                    voteFor = my_id;
                    granted_vote = 1;
                    auto args = RequestVoteArgs{current_term, my_id, log_storage->Size() - 1, log_storage->Back().term};
                    for (const auto &node_id: peer) {
                        thread_pool->enqueue(&RaftNode::send_request_vote, this, node_id, args);
                    }
                }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_commit() {
        while (!is_stopped()) {
            {
                std::this_thread::sleep_for(std::chrono::milliseconds{300});

                if (role != RaftRole::Leader || !rpc_clients_map[my_id]) {
                    if (!rpc_clients_map[my_id]) become_follower(current_term, -1);
                    continue;
                }

                for (const auto node_id : peer) {
                    if (!rpc_clients_map[node_id]) continue;
                    auto next_idx = next_index[node_id];
                    auto prev_idx = next_idx - 1;
                    if (prev_idx > log_storage->Size() - 1) {
                        continue;
                    }
                    auto prev_term = log_storage->At(prev_idx).term;
                    auto entries = log_storage->GetAllAfterIndex(prev_idx);
                    auto args = AppendEntriesArgs<Command>{
                            current_term, my_id, prev_idx, prev_term, commit_index, false, lastIncludeIndex, entries,
                    };
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id, args);
                }
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_apply() {
        while (!is_stopped()) {
            {
                std::this_thread::sleep_for(std::chrono::milliseconds{300});
                persist();
                for (int i = state->store.size(); i <= commit_index; ++i) {
                    auto entry = log_storage->At(i); // 只调用一次At
                    state->apply_log(entry.command); // 应用日志条目
                }
                state->num_append_logs = 0; // 重置追加日志的数量

            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_ping() {
        while (!is_stopped()) {
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (role == RaftRole::Leader)
                {
                    if (!rpc_clients_map[my_id]) {
                        become_follower(current_term, -1);
                        continue;
                    }
                    for (const auto &node_id: peer) {
                        if (node_id == my_id) continue;  // Skip sending to self.
                        AppendEntriesArgs<Command> args{
                                current_term,
                                my_id,
                                0,  // prevLogIndex would be set in a real scenario.
                                0,  // prevLogTerm would be set in a real scenario.
                                commit_index,
                                true,  // Indicate that this is a heartbeat.
                                lastIncludeIndex,
                                {}  // Empty entries for a heartbeat.
                        };
                        thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id, args);
                    }
                }
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_leader() {
        role = RaftRole::Leader;
        leaderId = my_id;
        granted_vote = 0;
        voteFor = -1;
        vote_timer.stop();
        persist();
        RAFT_LOG("become leader")
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_follower(int term, int id_leader) {
        //  RAFT_LOG("become follower")
        role = RaftRole::Follower;
        vote_timer.start();
        current_term = term;
        leaderId = id_leader;
        voteFor = -1;
        granted_vote = 0;
        persist();
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_candidate() {
        role = RaftRole::Candidate;
        vote_timer.start();
        voteFor = my_id;
        granted_vote = 1;
        persist();
        //  RAFT_LOG("become candidate")
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::persist() {
        std::vector<uint8_t> buffer(bm->block_size());
        try {
            auto block_num = log_storage->Persist();
            auto persist_data = RaftNodePersist{current_term, voteFor, block_num};
            *(RaftNodePersist *)buffer.data() = persist_data;
            bm->write_block(0, buffer.data());
        } catch (std::exception &e) {
            RAFT_LOG("error here")
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::recover() {
        std::vector<uint8_t> buffer(bm->block_size());
        bm->read_block(0, buffer.data());
        auto persist_data = *(RaftNodePersist *)(buffer.data());
        current_term = persist_data.term;
        voteFor = persist_data.voteFor;
        if (persist_data.block_num == 0) {
            RAFT_LOG("init")
            log_storage->Append({0, 0});
        } else {
            log_storage->Recover(persist_data.block_num);
        }
    }
/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        /* turn off network */
        if (!network_availability[my_id]) {
            for (auto &&client : rpc_clients_map) {
                if (client.second != nullptr) client.second.reset();
            }
            //    RAFT_LOG("disable2 %d", my_id)
            return;
        }

        for (auto node_network : network_availability) {
            int node_id = node_network.first;
            bool node_status = node_network.second;

            if (node_status && rpc_clients_map[node_id] == nullptr) {
                RaftNodeConfig target_config;
                for (auto config : node_configs) {
                    if (config.node_id == node_id) target_config = config;
                }

                rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
            }

            if (!node_status && rpc_clients_map[node_id] != nullptr) {
                //      RAFT_LOG("disable %d", node_id)
                rpc_clients_map[node_id].reset();
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        for (auto &&client : rpc_clients_map) {
            if (client.second) {
                client.second->set_reliable(flag);
            }
        }
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::get_list_state_log_num() {
        /* only applied to ListStateMachine*/
        std::unique_lock<std::mutex> lock(mtx);

        return state->num_append_logs;
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::rpc_count() {
        int sum = 0;
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        for (auto &&client : rpc_clients_map) {
            if (client.second) {
                sum += client.second->count();
            }
        }

        return sum;
    }

    template <typename StateMachine, typename Command>
    std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
        if (is_stopped()) {
            return std::vector<u8>{};
        }

        std::unique_lock<std::mutex> lock(mtx);

        return state->snapshot();
    }

}  // namespace chfs