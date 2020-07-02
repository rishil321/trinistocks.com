from django.contrib.sitemaps import Sitemap
from . import models
from django.urls import reverse


class StaticViewSitemap(Sitemap):
    priority = 0.5
    changefreq = 'daily'

    def items(self):
        return ['covid19:totals', 'covid19:about', 'covid19:daily']

    def location(self, item):
        return reverse(item)
