import unittest

from tools.ssh_with_duo import _is_duo_prompt_visible


class SshWithDuoTests(unittest.TestCase):
    def test_detects_plain_duo_prompt(self) -> None:
        self.assertTrue(_is_duo_prompt_visible("Passcode or option (1-3): "))

    def test_detects_duo_prompt_with_carriage_return_and_ansi(self) -> None:
        prompt = (
            "\x1b[?2004l"
            "(jdominsk@flux-login2) Duo two-factor login for jdominsk\r\n"
            "\r\n"
            "Passcode or option (1-3): "
        )
        self.assertTrue(_is_duo_prompt_visible(prompt))

    def test_ignores_non_prompt_lines(self) -> None:
        self.assertFalse(_is_duo_prompt_visible("Success. Logging you in...\n"))


if __name__ == "__main__":
    unittest.main()
