from PyQt6.QtWidgets import QLabel

from smseventlog.gui.dialogs.base import InputField, InputForm


class PasswordPrompt(InputForm):
    def __init__(self, id_type='Username', prompt=''):
        super().__init__(window_title='Input Credentials', enforce_all=True)
        layout = self.v_layout
        prompt = f'{prompt}:\n\n(Passwords are always encrypted before storage).\n'
        layout.insertWidget(0, QLabel(prompt))

        self.add_input(field=InputField(text=id_type.title()))
        self.add_input(field=InputField(text='Password'))


class TSIUserName(PasswordPrompt):
    def __init__(self):
        prompt = 'To use the automated TSI system,\
            \nplease enter your username and password for www.komatsuamerica.net'

        super().__init__(prompt=prompt)


class SMSUserName(PasswordPrompt):
    def __init__(self):
        prompt = 'Please enter your SMS username (email) and password'

        super().__init__(prompt=prompt)


class ExchangeLogin(PasswordPrompt):
    def __init__(self, parent=None):
        prompt = 'Please enter your Exchange email and password'

        super().__init__(id_type='email', prompt=prompt)


class SuncorWorkRemote(PasswordPrompt):
    def __init__(self, parent=None):
        prompt = 'Please enter your Suncor uesrname (without @email.com) and password'

        super().__init__(prompt=prompt)
