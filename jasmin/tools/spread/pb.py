from hashlib import md5
from zope.interface import implementer
from twisted.spread.pb import challenge, IJellyable, AsReferenceable, IPerspective
from twisted.cred.credentials import IUsernameHashedPassword, Anonymous
from twisted.spread.flavors import IPBRoot, Referenceable
from twisted.cred.error import UnhandledCredentials, UnauthorizedLogin


class _JellyableAvatarMixin:
    """
    A helper mixin for PB avatars that ensures the returned avatar is
    jellyable and handles disconnection notifications for logout.
    """

    def _cb_login(self, result):
        """
        Called on successful portal.login().

        Ensures avatar is jellyable, arranges for logout to be called
        once (whether on disconnect or explicit logout), and tracks the avatar
        with the broker.
        """
        (interface, avatar, logout) = result

        if not IJellyable.providedBy(avatar):
            # Make sure the avatar is referenceable over PB
            avatar = AsReferenceable(avatar, "perspective")

        puid = avatar.processUniqueID()

        # Wrap logout so it's only called once.
        logout_ref = [logout]

        def maybeLogout():
            if logout_ref:
                fn = logout_ref.pop()
                fn()

        # Ensure logout is called on disconnect or explicit logout
        self.broker._localCleanup[puid] = maybeLogout
        self.broker.notifyOnDisconnect(maybeLogout)

        return avatar

    def _login_error(self, err, username='Anonymous'):
        """
        Called when login fails. Transforms known exceptions into a tuple
        of (success=False, message) and logs appropriately.
        """
        if err.type == UnhandledCredentials:
            if str(err.value) == 'No checker for twisted.cred.credentials.IAnonymous':
                self.log.info('Anonymous connection is not authorized!')
                return False, 'Anonymous connection is not authorized!'
            else:
                self.log.info('Authentication error for user: %s', username)
                return False, f'Authentication error: {username}'
        elif err.type == UnauthorizedLogin:
            self.log.info('Authentication error for user: %s', username)
            return False, f'Authentication error: {username}'
        else:
            # Unknown error
            self.log.error('Unknown authentication error: %s', err)
            return False, f'Unknown authentication error: {err}'


@implementer(IUsernameHashedPassword)
class _PortalAuthVerifier(Referenceable, _JellyableAvatarMixin):
    """
    Used in the login sequence to verify a hashed password.

    This differs from twisted.spread.pb._PortalAuthChallenger in that the server
    holds MD5 digested passwords (no plaintext), and we verify by recomputing the
    MD5 hash with the challenge and comparing to the received response.
    """

    def __init__(self, portal, broker, username, _challenge):
        self.portal = portal
        self.broker = broker
        self.username = username
        self.challenge = _challenge

        # Use the PBFactory's logger from the realm
        self.log = self.portal.realm.PBFactory.log

    def remote_respond(self, response, mind):
        """
        Called by the client with their response to the challenge.
        On success, portal.login() returns a deferred that upon callback,
        we transform using _cb_login to return a jellyable avatar.
        """
        self.response = response
        d = self.portal.login(self, mind, IPerspective)
        d.addCallback(self._cb_login)
        d.addErrback(self._login_error, self.username)
        return d

    def checkPassword(self, md5password):
        """
        Verifies the provided MD5 hashed password by recomputing
        MD5 with the challenge and comparing to the client's response.
        """
        hasher = md5()
        hasher.update(md5password)
        hasher.update(self.challenge)
        correct = hasher.digest()
        return self.response == correct


class _PortalWrapper(Referenceable, _JellyableAvatarMixin):
    """
    Root Referenceable object used to initiate the login process through the portal.
    """

    def __init__(self, portal, broker):
        self.portal = portal
        self.broker = broker

        # Use the PBFactory's logger from the realm
        self.log = self.portal.realm.PBFactory.log

    def remote_login(self, username):
        """
        Initiates a username/password login challenge.

        Returns a challenge and an _PortalAuthVerifier to handle the response.
        """
        c = challenge()
        return c, _PortalAuthVerifier(self.portal, self.broker, username, c)

    def remote_loginAnonymous(self, mind):
        """
        Attempt an anonymous login.

        Returns a deferred that fires with a jellyable avatar on success,
        or an error tuple on failure.
        """
        d = self.portal.login(Anonymous(), mind, IPerspective)
        d.addCallback(self._cb_login)
        d.addErrback(self._login_error)
        return d


@implementer(IPBRoot)
class JasminPBPortalRoot:
    """
    Root object that PB uses to get the initial Referenceable which is an instance
    of _PortalWrapper, enabling the login process.
    """

    def __init__(self, portal):
        self.portal = portal

    def rootObject(self, broker):
        """
        Called by PB when a connection is made to get the root object.
        Returns an instance of _PortalWrapper to handle login requests.
        """
        return _PortalWrapper(self.portal, broker)
