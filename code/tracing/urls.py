"""URL patterns for the narrative authoring system."""

from django.urls import path
from . import views

app_name = "narratives"

urlpatterns = [
    # Page views
    path("",                                        views.gallery,        name="gallery"),

    # ID-scoped routes — must come before the bare <vis_type>/ catch-all so that
    # /narratives/Bar/42/ and /narratives/jsonld/42/ resolve correctly.
    path("new/",                                       views.author_new,     name="author_new"),
    path("jsonld/<int:narrative_id>/",              views.serve_jsonld,   name="serve_jsonld"),
    path("<str:vis_type>/<int:narrative_id>/",      views.detail,         name="detail_by_id"),

    # vis_type-only routes
    path("<str:vis_type>/",                         views.detail,         name="detail"),
    path("<str:vis_type>/author/",                  views.author,         name="author"),

    # API endpoints
    path("<str:vis_type>/check-similar/",           views.check_similar,  name="check_similar"),
    path("<str:vis_type>/generate/",                views.generate,       name="generate"),
    path("<str:vis_type>/add-chart/",               views.add_chart,      name="add_chart"),
    path("<str:vis_type>/delete-block/",            views.delete_block,   name="delete_block"),
    path("<str:vis_type>/reorder/",                 views.reorder,        name="reorder"),
    path("<str:vis_type>/regen-chart/",             views.regen_chart,    name="regen_chart"),
    path("<str:vis_type>/publish/",                 views.publish,        name="publish"),
    path("<str:vis_type>/view/",                    views.increment_view, name="increment_view"),
    path("<str:vis_type>/reset-draft/",             views.reset_draft,    name="reset_draft"),
    path("<str:vis_type>/figures-pool/",            views.figures_pool,   name="figures_pool"),
    path("<str:vis_type>/update-figures/",          views.update_figures, name="update_figures"),
]