"""
Custom template filters for the ui app.

Load with: {% load ui_extras %} at the top of any template that uses these.
"""
from django import template
from core.taxonomy import vis_type_to_slug

register = template.Library()


@register.filter
def split(value, delimiter=","):
    """Split a string by delimiter. Usage: {{ "a,b,c"|split:"," }}"""
    return str(value).split(delimiter)


@register.filter
def get_item(dictionary, key):
    """Dict lookup by variable key. Usage: {{ mydict|get_item:key }}"""
    if isinstance(dictionary, dict):
        return dictionary.get(key)
    return None


@register.filter
def vis_slug(value):
    """Return the canonical narrative slug for a vis_type label."""
    return vis_type_to_slug(str(value or ""))
