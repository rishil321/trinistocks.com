from django.urls import path
from django.contrib.auth.models import User
from rest_framework.urlpatterns import format_suffix_patterns
from . import views

app_name = 'covid19'

urlpatterns = [
    path('', views.Covid19_Paho_Reports_List.as_view({'get': 'list'}), name='totals'),
    path('<int:pk>/', views.Covid19_Paho_Reports_Detail.as_view({'get': 'retrieve'}), name='totals2'),
    path('totals', views.totals, name='totals'),
    path('about', views.about, name='about'),
    path('daily', views.daily, name='daily'),
]

urlpatterns = format_suffix_patterns(urlpatterns)