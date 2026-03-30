"""URL patterns for the narrative authoring system."""

from django.urls import path
from . import views

app_name = "narratives"

urlpatterns = [
    # Page views
    path("",                                        views.gallery,        name="gallery"),

    # ID-scoped routes — keep these before the vis_type routes so that
    # /narratives/bar/42/ and /narratives/jsonld/42/ resolve correctly.
    path("new/",                                       views.author_new,     name="author_new"),
    path("jsonld/<int:narrative_id>/",              views.serve_jsonld,   name="serve_jsonld"),
    path("<path:vis_type>/<int:narrative_id>/",     views.detail,         name="detail_by_id"),

    # vis_type routes — specific suffixes must come before the bare detail route
    # because <path:vis_type> may itself contain slashes (e.g. Radar/Spider).
    path("<path:vis_type>/author/",                 views.author,         name="author"),

    # API endpoints
    path("<path:vis_type>/check-similar/",          views.check_similar,  name="check_similar"),
    path("<path:vis_type>/generate/",               views.generate,       name="generate"),
    path("<path:vis_type>/add-chart/",              views.add_chart,      name="add_chart"),
    path("<path:vis_type>/delete-block/",           views.delete_block,   name="delete_block"),
    path("<path:vis_type>/reorder/",                views.reorder,        name="reorder"),
    path("<path:vis_type>/regen-chart/",            views.regen_chart,    name="regen_chart"),
    path("<path:vis_type>/publish/",                views.publish,        name="publish"),
    path("<path:vis_type>/view/",                   views.increment_view, name="increment_view"),
    path("<path:vis_type>/reset-draft/",            views.reset_draft,    name="reset_draft"),
    path("<path:vis_type>/figures-pool/",           views.figures_pool,   name="figures_pool"),
    path("<path:vis_type>/update-figures/",         views.update_figures, name="update_figures"),
    path("<path:vis_type>/",                        views.detail,         name="detail"),
]
