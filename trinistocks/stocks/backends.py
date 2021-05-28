from django.contrib.auth import get_user_model
from django.contrib.auth.backends import ModelBackend
from django.core.exceptions import ObjectDoesNotExist


class EmailAuthBackend(ModelBackend):
    def authenticate(self, request, username=None, password=None, email=None, **kwargs):
        UserModel = get_user_model()
        try:
            if email:
                user = UserModel.objects.get(email=email)
            elif username:
                user = UserModel.objects.get(username=username)
            else:
                raise ObjectDoesNotExist
        except UserModel.DoesNotExist:
            return None
        else:
            if user.check_password(password):
                return user
        return None