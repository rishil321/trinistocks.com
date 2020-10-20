from django import forms
from django.core.exceptions import ValidationError
from django.contrib.auth.models import User
import django.core.validators as validators
import django.contrib.auth.password_validation as password_validators
from django.contrib.auth import authenticate, login

class RegisterForm(forms.Form):
    username = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))
    email = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))
    password = forms.CharField(widget=forms.PasswordInput(attrs={'class' : 'input'}))

    def send_email(self):
        # send email using the self.cleaned_data dictionary
        pass

    def clean_username(self):
        data = self.cleaned_data['username']
        # check if this username exists already
        try:
            user_check = User.objects.get(username=data)
            # if an exception is not raised, then this user already exists
            raise ValidationError("Your username already exists. Please choose another.")
        except User.DoesNotExist:
            # continue since this user does not already exist. 
            pass
        return data

    def clean_email(self):
        data = self.cleaned_data['email']
        # check if this email exists already
        try:
            email_check = User.objects.get(email=data)
            # if an exception is not raised, then this user already exists
            raise ValidationError("This email address has already been used. Please choose another or reset the password for this user.")
        except User.DoesNotExist:
            # continue since this user does not already exist. 
            pass
        # check if this is a valid email address
        validators.validate_email(data)
        # if no exception is raised, this is a valid email
        pass
        return data

    def clean_password(self):
        data = self.cleaned_data['password']
        # check if this password is good
        password_validators.validate_password(data)
        return data

class LoginForm(forms.Form):
    username = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))
    password = forms.CharField(widget=forms.PasswordInput(attrs={'class' : 'input'}))

    def send_email(self):
        # send email using the self.cleaned_data dictionary
        pass

    def clean_username(self):
        try:
            data = self.cleaned_data['username']
            # check if this email exists
            User.objects.get(username=data)
            # if an exception is not raised, then this user exists
        except KeyError:
            raise ValidationError("Please enter a username.")
        except User.DoesNotExist:
            raise ValidationError("This username could not be found. Please recheck.")
        return data

    def clean_password(self):
        try:
            username = self.cleaned_data['username']
            password = self.cleaned_data['password']
        except KeyError:
            raise ValidationError("Please enter both a username and a password.")
        # check if this password is correct
        user = authenticate(username=username,password=password)
        if user is not None:
            pass
        else:
            raise ValidationError("Your password was incorrect. Please recheck.")
        return password


class PortfolioTransactionForm(forms.Form):
    symbol = forms.CharField(widget=forms.Select(attrs={'class' : 'custom-dropdown'}))
    num_shares = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))
    bought_or_sold =  forms.CharField(widget=forms.Select(attrs={'class' : 'custom-dropdown'}))
    price = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))