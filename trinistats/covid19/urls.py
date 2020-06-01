from django.urls import path
from django.conf.urls import url
from django.contrib.auth.models import User
from rest_framework.urlpatterns import format_suffix_patterns
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
from . import views

app_name = 'covid19'

schema_view = get_schema_view(
    openapi.Info(
        title="Snippets API",
        default_version='v1',
        description="Test description",
        terms_of_service="https://www.google.com/policies/terms/",
        contact=openapi.Contact(email="contact@snippets.local"),
        license=openapi.License(name="BSD License"),
    ),
    public=True,
    permission_classes=(permissions.IsAuthenticatedOrReadOnly,),
)

urlpatterns = [
    path('totals', views.totals, name='totals'),
    path('about', views.about, name='about'),
    path('daily', views.daily, name='daily'),
    path('api/paho',
         views.Covid19_Paho_Reports_List.as_view({'get': 'list'}), name='api_paho'),
    path('api/worldometers', views.Covid19_Worldometers_Reports_List.as_view(
        {'get': 'list'}), name='api_worldometers'),
    path('api/docs', schema_view.with_ui('swagger',
                                         cache_timeout=0), name='swagger_ui'),
]

urlpatterns = format_suffix_patterns(urlpatterns)
