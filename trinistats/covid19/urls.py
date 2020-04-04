from django.urls import path

from . import views

urlpatterns = [
    path('', views.totals, name='totals'),
    path('totals', views.totals, name='totals'),
    path('about', views.about, name='about'),
    path('daily', views.daily, name='daily'),
]