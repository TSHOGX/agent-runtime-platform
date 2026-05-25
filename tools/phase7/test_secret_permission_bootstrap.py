#!/usr/bin/env python3
import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("bootstrap-secret-permissions.py")
SPEC = importlib.util.spec_from_file_location("secret_permission_bootstrap", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SecretPermissionBootstrapTest(unittest.TestCase):
    def test_build_commands_bootstraps_missing_group_owner_and_agent_membership(self):
        settings = MODULE.Settings(
            secrets_root="/var/lib/harness/secrets",
            readers_gid=65501,
            owner_user="orchestrator",
            owner_home="/var/lib/harness",
            readers_group="harness-secret-readers",
            agent_uid=65534,
            agent_user="",
        )
        state = MODULE.AccountState(
            users_by_name={"nobody": MODULE.UserInfo("nobody", 65534, 65534)},
            users_by_uid={65534: (MODULE.UserInfo("nobody", 65534, 65534),)},
            groups_by_name={},
            groups_by_gid={},
        )

        commands, agent_user = MODULE.build_commands(settings, state)

        self.assertEqual(agent_user, "nobody")
        self.assertEqual(
            commands,
            [
                ["groupadd", "--gid", "65501", "harness-secret-readers"],
                [
                    "useradd",
                    "--system",
                    "--home-dir",
                    "/var/lib/harness",
                    "--shell",
                    "/usr/sbin/nologin",
                    "--no-create-home",
                    "orchestrator",
                ],
                ["usermod", "-a", "-G", "harness-secret-readers", "nobody"],
                ["mkdir", "-p", "/var/lib/harness/secrets"],
                ["chown", "orchestrator:harness-secret-readers", "/var/lib/harness/secrets"],
                ["chmod", "0750", "/var/lib/harness/secrets"],
            ],
        )

    def test_build_commands_rejects_wrong_existing_group_gid(self):
        settings = MODULE.Settings(
            secrets_root="/var/lib/harness/secrets",
            readers_gid=65501,
            owner_user="orchestrator",
            owner_home="/var/lib/harness",
            readers_group="harness-secret-readers",
            agent_uid=65534,
            agent_user="nobody",
        )
        state = MODULE.AccountState(
            users_by_name={"nobody": MODULE.UserInfo("nobody", 65534, 65534)},
            users_by_uid={65534: (MODULE.UserInfo("nobody", 65534, 65534),)},
            groups_by_name={
                "harness-secret-readers": MODULE.GroupInfo("harness-secret-readers", 42, ("nobody",))
            },
            groups_by_gid={42: (MODULE.GroupInfo("harness-secret-readers", 42, ("nobody",)),)},
        )

        with self.assertRaisesRegex(RuntimeError, "gid 42, want 65501"):
            MODULE.build_commands(settings, state)

    def test_build_commands_rejects_gid_owned_by_another_group(self):
        settings = MODULE.Settings(
            secrets_root="/var/lib/harness/secrets",
            readers_gid=65501,
            owner_user="orchestrator",
            owner_home="/var/lib/harness",
            readers_group="harness-secret-readers",
            agent_uid=65534,
            agent_user="nobody",
        )
        other_group = MODULE.GroupInfo("other-secret-group", 65501, ())
        state = MODULE.AccountState(
            users_by_name={"nobody": MODULE.UserInfo("nobody", 65534, 65534)},
            users_by_uid={65534: (MODULE.UserInfo("nobody", 65534, 65534),)},
            groups_by_name={"other-secret-group": other_group},
            groups_by_gid={65501: (other_group,)},
        )

        with self.assertRaisesRegex(RuntimeError, "gid 65501 already belongs"):
            MODULE.build_commands(settings, state)

    def test_load_secret_config_reads_harness_section_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "harness.yaml"
            config.write_text(
                """
secrets:
  root: /wrong
  readers_gid: 1
harness:
  secrets:
    root: /var/lib/harness/secrets
    readers_gid: 65501
""",
                encoding="utf-8",
            )

            self.assertEqual(MODULE.load_secret_config(config), ("/var/lib/harness/secrets", 65501))


if __name__ == "__main__":
    unittest.main()
