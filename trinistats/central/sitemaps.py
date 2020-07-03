from django.contrib.sitemaps import Sitemap
from . import models
from django.urls import reverse


class StaticViewSitemap(Sitemap):
    priority = 0.5
    changefreq = 'daily'

    def items(self):
        return [
            'covid19:totals', 'covid19:about', 'covid19:daily',
            'stocks:landingpage', 'stocks:dailytradingsummary', 'stocks:listedstocks',
            'stocks:technicalanalysis', 'stocks:marketindexhistory', 'stocks:stockhistory',
            'stocks:dividendhistory', 'stocks:dividendyieldhistory', 'stocks:ostradeshistory',
            'stocks:about']

    def location(self, item):
        return reverse(item)
