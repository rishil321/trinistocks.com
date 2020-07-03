from django.urls import path
from . import views
from django.contrib.sitemaps.views import sitemap
from . import sitemaps

app_name = 'central'

sitemaps = {
    "static": sitemaps.StaticViewSitemap,
}

urlpatterns = [
    path("sitemap.xml", sitemap, {"sitemaps": sitemaps}, name="sitemap"),
    path('', views.landingpage, name='landingpage'),
]
