"""
Django settings for paper_to_practice MVP.
Reads all secrets and config from .env via python-decouple.
"""
import os
from pathlib import Path
from decouple import config, Csv

# ── Kaggle credentials — must be in os.environ before kaggle is imported ───────
# python-decouple does not auto-populate os.environ, so we push these explicitly.
# kaggle 2.0 instantiates its api object at import time and reads os.environ then.
# Uses legacy API credentials (KAGGLE_KEY) — generate at kaggle.com → Settings →
# API → Legacy API Credentials → Create New Token.
_kaggle_username = config("KAGGLE_USERNAME", default="")
_kaggle_key = config("KAGGLE_KEY", default="")
if _kaggle_username:
    os.environ.setdefault("KAGGLE_USERNAME", _kaggle_username)
if _kaggle_key:
    os.environ.setdefault("KAGGLE_KEY", _kaggle_key)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

# ── Core security ──────────────────────────────────────────────────────────────
SECRET_KEY = config("SECRET_KEY", default="dev-insecure-key-change-in-production")
DEBUG = config("DEBUG", default=True, cast=bool)
ALLOWED_HOSTS = config("ALLOWED_HOSTS", default="127.0.0.1,localhost", cast=Csv())

# ── Installed apps ─────────────────────────────────────────────────────────────
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # Project apps
    "core",
    "academic",
    "repository",
    "tracing",
    "ui",
]

# ── Middleware ─────────────────────────────────────────────────────────────────
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

# ── Templates ─────────────────────────────────────────────────────────────────
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "ui" / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

# ── Database ───────────────────────────────────────────────────────────────────
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

# ── Password validation ────────────────────────────────────────────────────────
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# ── Internationalisation ───────────────────────────────────────────────────────
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

# ── Static files ───────────────────────────────────────────────────────────────
STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

# ── Media files (PDF downloads, extracted figures, notebooks) ─────────────────
MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"

# ── Default primary key field ─────────────────────────────────────────────────
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ── Logging ───────────────────────────────────────────────────────────────────
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[{asctime}] {levelname} {name}: {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
    "loggers": {
        "django": {
            "handlers": ["console"],
            "level": "WARNING",
            "propagate": False,
        },
        "core": {"handlers": ["console"], "level": "DEBUG", "propagate": False},
        "academic": {"handlers": ["console"], "level": "DEBUG", "propagate": False},
        "repository": {"handlers": ["console"], "level": "DEBUG", "propagate": False},
        "tracing": {"handlers": ["console"], "level": "DEBUG", "propagate": False},
        "ui": {"handlers": ["console"], "level": "DEBUG", "propagate": False},
    },
}
