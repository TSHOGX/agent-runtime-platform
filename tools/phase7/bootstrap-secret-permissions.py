#!/usr/bin/env python3
import argparse
import json
import os
import pwd
import grp
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    secrets_root: str
    readers_gid: int
    owner_user: str
    owner_home: str
    readers_group: str
    agent_uid: int
    agent_user: str


@dataclass(frozen=True)
class UserInfo:
    name: str
    uid: int
    gid: int


@dataclass(frozen=True)
class GroupInfo:
    name: str
    gid: int
    members: tuple[str, ...]


@dataclass(frozen=True)
class AccountState:
    users_by_name: dict[str, UserInfo]
    users_by_uid: dict[int, tuple[UserInfo, ...]]
    groups_by_name: dict[str, GroupInfo]
    groups_by_gid: dict[int, tuple[GroupInfo, ...]]


def parse_args():
    parser = argparse.ArgumentParser(description="Bootstrap the Phase 7 host secret permission model.")
    parser.add_argument("--config", default=os.environ.get("PHASE7_CONFIG", "config/harness.yaml"))
    parser.add_argument("--secrets-root", default=os.environ.get("PHASE7_SECRETS_ROOT", ""))
    parser.add_argument("--readers-gid", type=int, default=int(os.environ.get("PHASE7_SECRET_READERS_GID", "0")))
    parser.add_argument("--owner-user", default=os.environ.get("PHASE7_SECRET_OWNER", "orchestrator"))
    parser.add_argument("--owner-home", default=os.environ.get("PHASE7_SECRET_OWNER_HOME", "/var/lib/harness"))
    parser.add_argument("--readers-group", default=os.environ.get("PHASE7_SECRET_READERS_GROUP", "harness-secret-readers"))
    parser.add_argument("--agent-uid", type=int, default=int(os.environ.get("PHASE7_AGENT_UID", "65534")))
    parser.add_argument("--agent-user", default=os.environ.get("PHASE7_AGENT_USER", ""))
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def load_secret_config(config_path):
    root = ""
    readers_gid = 0
    in_harness = False
    harness_indent = None
    harness_child_indent = None
    in_secrets = False
    secrets_indent = None
    with open(config_path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            stripped = line.strip()
            indent = len(line) - len(line.lstrip(" "))

            if stripped == "harness:":
                in_harness = True
                harness_indent = indent
                harness_child_indent = None
                in_secrets = False
                secrets_indent = None
                continue
            if in_harness and indent <= harness_indent:
                in_harness = False
                in_secrets = False
            if not in_harness:
                continue
            if harness_child_indent is None and indent > harness_indent:
                harness_child_indent = indent
            if stripped == "secrets:" and indent == harness_child_indent:
                in_secrets = True
                secrets_indent = indent
                continue
            if in_secrets and indent <= secrets_indent:
                in_secrets = False
            if not in_secrets or ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            value = value.strip().strip("'\"")
            if key == "root":
                root = value
            elif key == "readers_gid":
                readers_gid = int(value)
    return root, readers_gid


def settings_from_args(args):
    config_root, config_gid = load_secret_config(args.config)
    secrets_root = args.secrets_root or config_root
    readers_gid = args.readers_gid or config_gid
    if not secrets_root:
        raise RuntimeError("secrets root is required")
    if readers_gid <= 0:
        raise RuntimeError("secret readers gid must be > 0")
    return Settings(
        secrets_root=secrets_root,
        readers_gid=readers_gid,
        owner_user=args.owner_user,
        owner_home=args.owner_home,
        readers_group=args.readers_group,
        agent_uid=args.agent_uid,
        agent_user=args.agent_user,
    )


def account_state_from_system():
    users_by_name = {}
    users_by_uid = {}
    for entry in pwd.getpwall():
        info = UserInfo(entry.pw_name, entry.pw_uid, entry.pw_gid)
        users_by_name[info.name] = info
        users_by_uid.setdefault(info.uid, []).append(info)
    groups_by_name = {}
    groups_by_gid = {}
    for entry in grp.getgrall():
        info = GroupInfo(entry.gr_name, entry.gr_gid, tuple(entry.gr_mem))
        groups_by_name[info.name] = info
        groups_by_gid.setdefault(info.gid, []).append(info)
    return AccountState(
        users_by_name=users_by_name,
        users_by_uid={uid: tuple(users) for uid, users in users_by_uid.items()},
        groups_by_name=groups_by_name,
        groups_by_gid={gid: tuple(groups) for gid, groups in groups_by_gid.items()},
    )


def group_members(state, group):
    members = set(group.members)
    for user in state.users_by_name.values():
        if user.gid == group.gid:
            members.add(user.name)
    return members


def resolve_agent_user(settings, state):
    if settings.agent_user:
        user = state.users_by_name.get(settings.agent_user)
        if user is None:
            raise RuntimeError(f"agent user {settings.agent_user!r} does not exist")
        if user.uid != settings.agent_uid:
            raise RuntimeError(f"agent user {settings.agent_user!r} uid {user.uid}, want {settings.agent_uid}")
        return user.name
    candidates = sorted(state.users_by_uid.get(settings.agent_uid, ()), key=lambda user: user.name)
    if not candidates:
        raise RuntimeError(f"no local user has uid {settings.agent_uid}")
    for user in candidates:
        if user.name == "nobody":
            return user.name
    return candidates[0].name


def build_commands(settings, state):
    commands = []
    group = state.groups_by_name.get(settings.readers_group)
    if group is None:
        existing_groups = state.groups_by_gid.get(settings.readers_gid, ())
        if existing_groups:
            names = ", ".join(sorted(group.name for group in existing_groups))
            raise RuntimeError(f"gid {settings.readers_gid} already belongs to group(s): {names}")
        commands.append(["groupadd", "--gid", str(settings.readers_gid), settings.readers_group])
        current_members = set()
    elif group.gid != settings.readers_gid:
        raise RuntimeError(f"group {settings.readers_group!r} gid {group.gid}, want {settings.readers_gid}")
    else:
        current_members = group_members(state, group)

    if settings.owner_user not in state.users_by_name:
        commands.append(
            [
                "useradd",
                "--system",
                "--home-dir",
                settings.owner_home,
                "--shell",
                "/usr/sbin/nologin",
                "--no-create-home",
                settings.owner_user,
            ]
        )

    agent_user = resolve_agent_user(settings, state)
    if agent_user not in current_members:
        commands.append(["usermod", "-a", "-G", settings.readers_group, agent_user])

    commands.extend(
        [
            ["mkdir", "-p", settings.secrets_root],
            ["chown", f"{settings.owner_user}:{settings.readers_group}", settings.secrets_root],
            ["chmod", "0750", settings.secrets_root],
        ]
    )
    return commands, agent_user


def run_commands(commands):
    for command in commands:
        subprocess.run(command, check=True)


def main():
    args = parse_args()
    settings = settings_from_args(args)
    commands, agent_user = build_commands(settings, account_state_from_system())
    if args.apply and os.geteuid() != 0:
        raise RuntimeError("--apply must run as root")
    if args.apply:
        run_commands(commands)
    print(
        json.dumps(
            {
                "result": "applied" if args.apply else "dry_run",
                "secrets_root": settings.secrets_root,
                "owner_user": settings.owner_user,
                "readers_group": settings.readers_group,
                "readers_gid": settings.readers_gid,
                "agent_uid": settings.agent_uid,
                "agent_user": agent_user,
                "commands": commands,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"phase7 secret permission bootstrap failed: {err}", file=sys.stderr)
        raise SystemExit(1)
