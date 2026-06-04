TEST_BITHUMB_API_KEY = "test-api-key"
TEST_BITHUMB_API_SECRET = "test-secret-for-hs256-min-32-bytes-v1"

assert len(TEST_BITHUMB_API_SECRET.encode("utf-8")) >= 32


def configure_bithumb_test_auth(settings_obj: object) -> None:
    object.__setattr__(settings_obj, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings_obj, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
