from django.urls import path

from . import views

urlpatterns = [
    path('', views.cases, name='cases'),
    path('about', views.about, name='about'),
]