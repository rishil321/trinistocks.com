import logging

from django.core.mail import send_mail
from django.dispatch import receiver
from django.template.loader import render_to_string
from django.urls import reverse
from django_rest_passwordreset.signals import reset_password_token_created

LOGGER = logging.getLogger("root")


@receiver(reset_password_token_created)
def password_reset_token_created(
    sender, instance, reset_password_token, *args, **kwargs
):
    """
    Handles password reset tokens
    When a token is created, an e-mail needs to be sent to the user
    :param sender: View Class that sent the signal
    :param instance: View Instance that sent the signal
    :param reset_password_token: Token Model Object
    :param args:
    :param kwargs:
    :return:
    """
    # send an e-mail to the user
    context = {
        "current_user": reset_password_token.user,
        "username": reset_password_token.user.username,
        "email": reset_password_token.user.email,
        "token": reset_password_token.key,
    }

    # render email text
    email_plaintext_message = render_to_string(
        "stocks/account/api_password_reset_email.txt", context
    )
    send_mail(
        "Reset password for trinistocks.com",
        email_plaintext_message,
        "admin@trinistocks.com",
        [context["email"]],
        fail_silently=False,
    )
    LOGGER.debug(f"Sent mail to {reset_password_token.user.email} successfully.")
