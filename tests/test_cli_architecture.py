from tests.test_cli_structure import (
    test_app_impl_main_compatibility_smoke,
    test_app_impl_module_remains_deprecated_compatibility_facade,
    test_app_main_compatibility_smoke,
    test_app_module_remains_tiny_compatibility_shim,
    test_cli_command_modules_do_not_depend_on_app_impl_or_call_helper,
    test_cli_composition_modules_do_not_import_domain_internals,
    test_legacy_command_module_cannot_dispatch_to_app_impl,
)
