# Imports from standard libraries (
from django.urls import path, include, reverse_lazy
from django.contrib.auth import views as auth_views
from django.contrib.staticfiles.storage import staticfiles_storage
from django.views.generic.base import RedirectView
from django.conf import settings
from django.conf.urls.static import static
# Imports from local machine
from . import views

# Declare the app name
app_name = 'stocks'

# Set up valid URL patterns )
urlpatterns = [
    path('', views.HomePageView.as_view(), name="homepage"),
    path('dailytradingsummary', views.DailyTradingSummaryView.as_view(),
         name='dailytradingsummary'),
    path('listedstocks', views.ListedStocksView.as_view(),
         name='listedstocks'),
    path('technicalanalysis', views.TechnicalAnalysisSummary.as_view(),
         name='technicalanalysis'),
    path('fundamentalanalysis', views.FundamentalAnalysisSummary.as_view(),
         name='fundamentalanalysis'),
    path('fundamentalhistory', views.FundamentalHistoryView.as_view(),
         name='fundamentalhistory'),
    path('marketindexhistory', views.MarketIndexHistoryView.as_view(),
         name='marketindexhistory'),
    path('stockhistory', views.StockHistoryView.as_view(), name='stockhistory'),
    path('dividendhistory', views.DividendHistoryView.as_view(),
         name='dividendhistory'),
    path('ostradeshistory', views.OSTradesHistoryView.as_view(),
         name='ostradeshistory'),
    path('stocknewshistory', views.StockNewsHistoryView.as_view(),
         name='stocknewshistory'),
    path('about', views.AboutPageView.as_view(),
         name='about'),
    path('login', views.LoginPageView.as_view(),
         name='login'),
    path('logout', views.LogoutPageView.as_view(),
         name='logout'),
    path('register', views.RegisterPageView.as_view(),
         name='register'),
    path('portfoliotransactions', views.PortfolioTransactionsView.as_view(),
         name='portfoliotransactions'),
    path('portfoliosummary', views.PortfolioSummaryView.as_view(),
         name='portfoliosummary'),
    path('userprofile', views.UserProfileView.as_view(),
         name='userprofile'),
    path("account/password_reset", views.PasswordResetRequestView.as_view(),
         name="password_reset_request"),
    path('account/password_reset_confirm/<uidb64>/<token>', auth_views.PasswordResetConfirmView.as_view(template_name="stocks/account/base_passwordresetconfirm.html",
                                                                                                        success_url=reverse_lazy('stocks:password_reset_complete', current_app="stocks")), name='password_reset_confirm'),
    path('account/password_reset_complete', auth_views.PasswordResetCompleteView.as_view(
        template_name='stocks/account/base_passwordresetcomplete.html'), name='password_reset_complete'),
    path('account/password_change', auth_views.PasswordChangeView.as_view(template_name='stocks/account/base_passwordchange.html',
                                                                          success_url=reverse_lazy('stocks:password_change_complete', current_app="stocks")), name='password_change'),
    path('account/password_change_complete', auth_views.PasswordChangeDoneView.as_view(
        template_name='stocks/account/base_passwordchangecomplete.html'), name='password_change_complete'),
    path("ads.txt", RedirectView.as_view(
        url=staticfiles_storage.url("stocks/ads.txt")),),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
