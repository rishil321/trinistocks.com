def setup_django_orm():
    import sys
    sys.dont_write_bytecode = True
    # Django specific settings
    import os
    import pathlib
    sys.path.extend(
        [
            str(pathlib.Path(__file__).parent.parent.parent.resolve()),
            str(pathlib.Path(__file__).resolve().parent.resolve().parent.resolve().parent),
        ]
    )
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "trinistocks.settings")
    import django
    django.setup()
