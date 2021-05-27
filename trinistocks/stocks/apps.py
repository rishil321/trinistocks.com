from django.apps import AppConfig


class StocksConfig(AppConfig):
    name = "stocks"

    def ready(self):
        import stocks.signals
