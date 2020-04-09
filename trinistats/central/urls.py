from django.urls import path

from . import views

app_name = 'central'

urlpatterns = [
    path('', views.landingpage, name='landingpage'),
]