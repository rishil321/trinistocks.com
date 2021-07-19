# Imports from standard libraries (
from django.urls import path, include, reverse_lazy
from django.contrib.auth import views as auth_views
from django.contrib.staticfiles.storage import staticfiles_storage
from django.views.generic.base import RedirectView
from django.conf import settings
from django.conf.urls.static import static
from rest_framework.urlpatterns import format_suffix_patterns

# Imports from local machine
from . import views

# Declare the app name
app_name = "stocks"

# Set up valid URL patterns )
urlpatterns = [
    path("", views.HomePageView.as_view(), name="homepage"),
    path(
        "dailytradingsummary",
        views.DailyTradingSummaryView.as_view(),
        name="dailytradingsummary",
    ),
    path("listedstocks", views.ListedStocksView.as_view(), name="listedstocks"),
    path(
        "technicalanalysis",
        views.TechnicalAnalysisSummary.as_view(),
        name="technicalanalysis",
    ),
    path(
        "fundamentalanalysis",
        views.FundamentalAnalysisSummary.as_view(),
        name="fundamentalanalysis",
    ),
    path(
        "fundamentalhistory",
        views.FundamentalHistoryView.as_view(),
        name="fundamentalhistory",
    ),
    path(
        "marketindexhistory",
        views.MarketIndexHistoryView.as_view(),
        name="marketindexhistory",
    ),
    path("stockhistory", views.StockHistoryView.as_view(), name="stockhistory"),
    path(
        "dividendhistory", views.DividendHistoryView.as_view(), name="dividendhistory"
    ),
    path(
        "ostradeshistory", views.OSTradesHistoryView.as_view(), name="ostradeshistory"
    ),
    path(
        "stocknewshistory",
        views.StockNewsHistoryView.as_view(),
        name="stocknewshistory",
    ),
    path("about", views.AboutPageView.as_view(), name="about"),
    path("login", views.LoginPageView.as_view(), name="login"),
    path("logout", views.LogoutPageView.as_view(), name="logout"),
    path("register", views.RegisterPageView.as_view(), name="register"),
    path(
        "portfoliotransactions",
        views.PortfolioTransactionsView.as_view(),
        name="portfoliotransactions",
    ),
    path(
        "portfoliosummary",
        views.PortfolioSummaryView.as_view(),
        name="portfoliosummary",
    ),
    path("userprofile", views.UserProfileView.as_view(), name="userprofile"),
    path(
        "account/password_reset",
        views.PasswordResetRequestView.as_view(),
        name="password_reset_request",
    ),
    path(
        "account/password_reset_confirm/<uidb64>/<token>",
        auth_views.PasswordResetConfirmView.as_view(
            template_name="stocks/account/base_passwordresetconfirm.html",
            success_url=reverse_lazy(
                "stocks:password_reset_complete", current_app="stocks"
            ),
        ),
        name="password_reset_confirm",
    ),
    path(
        "account/password_reset_complete",
        auth_views.PasswordResetCompleteView.as_view(
            template_name="stocks/account/base_passwordresetcomplete.html"
        ),
        name="password_reset_complete",
    ),
    path(
        "account/password_change",
        auth_views.PasswordChangeView.as_view(
            template_name="stocks/account/base_passwordchange.html",
            success_url=reverse_lazy(
                "stocks:password_change_complete", current_app="stocks"
            ),
        ),
        name="password_change",
    ),
    path(
        "account/password_change_complete",
        auth_views.PasswordChangeDoneView.as_view(
            template_name="stocks/account/base_passwordchangecomplete.html"
        ),
        name="password_change_complete",
    ),
    path("api/latestdailytrades", views.DailyStocksTradedApiView.as_view()),
    path("api/stocknewsdata", views.StockNewsApiView.as_view()),
    path("api/listedstocks", views.ListedStocksApiView.as_view()),
    path("api/technicalanalysis", views.TechnicalAnalysisApiView.as_view()),
    path("api/fundamentalanalysis", views.FundamentalAnalysisApiView.as_view()),
    path("api/stockprices", views.StockPriceApiView.as_view()),
    path("api/lateststockprices", views.LatestStockPriceApiView.as_view()),
    path("api/dividendpayments", views.DividendPaymentsApiView.as_view()),
    path("api/dividendyields", views.DividendYieldsApiView.as_view()),
    path("api/marketindices", views.MarketIndicesApiView.as_view()),
    path("api/outstandingtrades", views.OutstandingTradesApiView.as_view()),
    path("api/portfoliosummary", views.PortfolioSummaryApiView.as_view()),
    path("api/portfoliosectors", views.PortfolioSectorsApiView.as_view()),
    path("api/portfoliotransaction", views.PortfolioTransactionsApiView.as_view()),
    path("api/simulatorgames", views.SimulatorGamesApiView.as_view()),
    path("api/simulatorplayers", views.SimulatorPlayersApiView.as_view()),
    path("api/simulatortransactions", views.SimulatorTransactionsApiView.as_view()),
    path("api/simulatorportfolios", views.SimulatorPortfoliosApiView.as_view()),
    path(
        "api/simulatorportfoliosectors",
        views.SimulatorPortfolioSectorsApiView.as_view(),
    ),
    path("api/createuser", views.UserCreate.as_view()),
    path("api/deleteuser", views.UserDelete.as_view()),
    path("api/usertoken", views.CustomAuthToken.as_view()),
    path("api/passwordchange", views.ChangePasswordView.as_view()),
    path(
        "ads.txt",
        RedirectView.as_view(url=staticfiles_storage.url("stocks/ads.txt")),
    ),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

urlpatterns = format_suffix_patterns(urlpatterns)
