import getpass


class Credentials(dict):
    def set_login(self, user=None, password=None):
        if user is None:
            print("Username: ")
            self['user'] = getpass._raw_input()
        else:
            self['user'] = user

        if password is None:
            self['password'] = getpass.getpass()
        else:
            self['password'] = password

    def get_login(self):
        return self.get('user', None), self.get('password', None)

