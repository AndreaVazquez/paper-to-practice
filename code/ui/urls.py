from django.urls import path
from . import views

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("drift-justifications/", views.encoding_justifications, name="drift_justifications"),
    path("encoding-justifications/", views.encoding_justifications, name="encoding_justifications"),
    path("explore/<str:vis_type>/", views.explore, name="explore"),
    path("activity/", views.activity, name="activity"),
    path("activity/stream/", views.activity_stream, name="activity_stream"),
    path("activity/status/", views.activity_status, name="activity_status"),
    path("run/<str:command>/", views.run_command, name="run_command"),
]
