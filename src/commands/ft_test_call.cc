/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cerrno>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTTestCallCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                           int argc) {
  if (argc < 2) {
    return absl::InvalidArgumentError("Usage: FT.TESTCALL <command> [args...]");
  }

  VMSDK_LOG(NOTICE, nullptr) << "DEBUG: start processing command";

  // Get the command name
  size_t cmd_len;
  const char *cmd_str = ValkeyModule_StringPtrLen(argv[1], &cmd_len);
  std::string command(cmd_str, cmd_len);

  int reply_count = 0;
  ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);

  ValkeyModule_ReplyWithSimpleString(ctx, "=== Testing ValkeyModule_Call ===");
  reply_count++;
  std::string cmd_msg = absl::StrFormat("Command: %s", command);
  ValkeyModule_ReplyWithSimpleString(ctx, cmd_msg.c_str());
  reply_count++;

  VMSDK_LOG(NOTICE, nullptr) << "DEBUG: finished processing command";

  // Test different commands based on input
  if (command == "CLUSTER_SLOTS") {
    VMSDK_LOG(NOTICE, nullptr) << "DEBUG: start CLUSTER_SLOTS part";
    auto reply = vmsdk::UniquePtrValkeyCallReply(
        ValkeyModule_Call(ctx, "CLUSTER", "c", "SLOTS"));
    VMSDK_LOG(NOTICE, nullptr) << "DEBUG: got reply from CLUSTER_SLOTS part";
    if (reply == nullptr) {
      std::string msg = absl::StrFormat("Result: NULL (errno=%d)", errno);
      ValkeyModule_ReplyWithSimpleString(ctx, msg.c_str());
      reply_count++;
    } else {
      auto reply_type = ValkeyModule_CallReplyType(reply.get());
      std::string type_msg = absl::StrFormat("Reply Type: %d", reply_type);
      ValkeyModule_ReplyWithSimpleString(ctx, type_msg.c_str());
      reply_count++;

      if (reply_type == VALKEYMODULE_REPLY_ERROR) {
        size_t len;
        const char *err_str =
            ValkeyModule_CallReplyStringPtr(reply.get(), &len);
        std::string err_msg =
            absl::StrFormat("Error: %s", std::string(err_str, len));
        ValkeyModule_ReplyWithSimpleString(ctx, err_msg.c_str());
        reply_count++;
      } else if (reply_type == VALKEYMODULE_REPLY_ARRAY) {
        size_t len = ValkeyModule_CallReplyLength(reply.get());
        std::string len_msg =
            absl::StrFormat("Number of slot ranges: %zu", len);
        ValkeyModule_ReplyWithSimpleString(ctx, len_msg.c_str());
        reply_count++;

        // Iterate through all slot ranges
        for (size_t i = 0; i < len; i++) {
          ValkeyModuleCallReply *slot_range =
              ValkeyModule_CallReplyArrayElement(reply.get(), i);
          if (!slot_range || ValkeyModule_CallReplyType(slot_range) !=
                                 VALKEYMODULE_REPLY_ARRAY) {
            continue;
          }

          size_t slot_len = ValkeyModule_CallReplyLength(slot_range);
          std::string separator = absl::StrFormat("--- Slot Range %zu ---", i);
          ValkeyModule_ReplyWithSimpleString(ctx, separator.c_str());
          reply_count++;

          // Get start and end slots
          if (slot_len >= 2) {
            ValkeyModuleCallReply *start_slot =
                ValkeyModule_CallReplyArrayElement(slot_range, 0);
            ValkeyModuleCallReply *end_slot =
                ValkeyModule_CallReplyArrayElement(slot_range, 1);

            long long start = ValkeyModule_CallReplyInteger(start_slot);
            long long end = ValkeyModule_CallReplyInteger(end_slot);

            std::string range_msg =
                absl::StrFormat("Slots: %lld to %lld", start, end);
            ValkeyModule_ReplyWithSimpleString(ctx, range_msg.c_str());
            reply_count++;
          }

          // Get node information (starting from index 2)
          for (size_t j = 2; j < slot_len; j++) {
            ValkeyModuleCallReply *node =
                ValkeyModule_CallReplyArrayElement(slot_range, j);
            if (!node ||
                ValkeyModule_CallReplyType(node) != VALKEYMODULE_REPLY_ARRAY) {
              continue;
            }

            size_t node_len = ValkeyModule_CallReplyLength(node);
            std::string node_type = (j == 2) ? "Master" : "Replica";

            if (node_len >= 2) {
              // Get IP
              ValkeyModuleCallReply *ip_reply =
                  ValkeyModule_CallReplyArrayElement(node, 0);
              size_t ip_len;
              const char *ip_str =
                  ValkeyModule_CallReplyStringPtr(ip_reply, &ip_len);
              std::string ip(ip_str, ip_len);

              // Get Port
              ValkeyModuleCallReply *port_reply =
                  ValkeyModule_CallReplyArrayElement(node, 1);
              long long port = ValkeyModule_CallReplyInteger(port_reply);

              // Get Node ID if available
              std::string node_id = "";
              if (node_len >= 3) {
                ValkeyModuleCallReply *id_reply =
                    ValkeyModule_CallReplyArrayElement(node, 2);
                size_t id_len;
                const char *id_str =
                    ValkeyModule_CallReplyStringPtr(id_reply, &id_len);
                node_id = std::string(id_str, id_len);
              }

              std::string node_msg;
              if (!node_id.empty()) {
                node_msg = absl::StrFormat("%s: %s:%lld (ID: %s)", node_type,
                                           ip, port, node_id);
              } else {
                node_msg = absl::StrFormat("%s: %s:%lld", node_type, ip, port);
              }
              ValkeyModule_ReplyWithSimpleString(ctx, node_msg.c_str());
              reply_count++;
            }
          }
        }
      }
    }
  } else {
    ValkeyModule_ReplyWithSimpleString(
        ctx, "Unknown test. Available: CLUSTER_SLOTS");
    reply_count++;
  }

  ValkeyModule_ReplySetArrayLength(ctx, reply_count);
  return absl::OkStatus();
}

}  // namespace valkey_search
