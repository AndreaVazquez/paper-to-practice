"""
Custom template filters for the ui app.

Load with: {% load ui_extras %} at the top of any template that uses these.
"""
from django import template

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
