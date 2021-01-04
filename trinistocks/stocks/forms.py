# imports from standard libs
from django import forms
from django.core.exceptions import ValidationError
import django.core.validators as validators
import django.contrib.auth.password_validation as password_validators
from django.contrib.auth import authenticate,get_user_model
# imports from local machine
from stocks import models

class RegisterForm(forms.Form):
    username = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))
    email = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))
    password = forms.CharField(widget=forms.PasswordInput(attrs={'class' : 'input'}))

    def clean_username(self):
        data = self.cleaned_data['username']
        # check if this username exists already
        try:
            user_check = get_user_model().objects.get(username=data)
            # if an exception is not raised, then this user already exists
            raise ValidationError("Your username already exists. Please choose another.")
        except models.User.DoesNotExist:
            # continue since this user does not already exist. 
            pass
        return data

    def clean_email(self):
        data = self.cleaned_data['email']
        # check if this email exists already
        try:
            email_check = get_user_model().objects.get(email=data)
            # if an exception is not raised, then this user already exists
            raise ValidationError("This email address has already been used. Please choose another or reset the password for this user.")
        except models.User.DoesNotExist:
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

    def clean_username(self):
        try:
            data = self.cleaned_data['username']
            # check if this email exists
            get_user_model().objects.get(username=data)
            # if an exception is not raised, then this user exists
        except KeyError:
            raise ValidationError("Please enter a username.")
        except models.User.DoesNotExist:
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

    # Set up the required data for this form
    ALL_LISTED_EQUITY_CHOICES = []
    choices = models.ListedEquities.objects.all().order_by('symbol')
    for choice in choices:
        ALL_LISTED_EQUITY_CHOICES.append((choice.symbol,choice.symbol))
    symbol = forms.ChoiceField(widget=forms.Select(attrs={'class' : ''}), choices = ALL_LISTED_EQUITY_CHOICES)
    num_shares = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))
    bought_or_sold =  forms.ChoiceField(widget=forms.Select(attrs={'class' : ''}), choices = (("Bought","Bought"),("Sold","Sold")))
    price = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))
    date = forms.DateField()

    def clean_num_shares(self):
        num_shares = self.cleaned_data['num_shares']
        try:
            num_shares = int(num_shares)
        except ValueError:
            raise ValidationError("You have not entered a valid number of shares. Please recheck.")
        return num_shares

    def clean_price(self):
        price = self.cleaned_data['price']
        try:
            # check if this is a valid float
            price = float(price)
            # check if this is positive
            if price <= 0.0:
                raise ValueError()
        except (ValueError,TypeError):
            raise ValidationError("You have not entered a valid price. Please recheck.")
        return price


class PasswordResetForm(forms.Form):
    email = forms.CharField(widget=forms.TextInput(attrs={'class' : 'input'}))

    def clean_email(self):
        data = self.cleaned_data['email']
        # check if this email exists in our db
        try:
            email_check = get_user_model().objects.get(email=data)
            # send email?
        except models.User.DoesNotExist:
            raise ValidationError("A user with this email address was not found.")
        # check if this is a valid email address
        validators.validate_email(data)
        # if no exception is raised, this is a valid email
        return data