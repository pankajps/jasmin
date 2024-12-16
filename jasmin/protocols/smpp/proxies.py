import pickle
from jasmin.tools.proxies import ConnectedPB, JasminPBProxy


class SMPPServerPBProxy(JasminPBProxy):
    """
    A proxy to the SMPPServerPB perspective broker, primarily used to deliver
    DLR and deliver_sm messages from a standalone process.
    """

    def _serialize_pdu(self, pdu):
        """
        Serialize the PDU object.

        This uses pickle by default but is designed to be easily replaced
        by another serialization mechanism (e.g., msgpack) in the future.
        """
        return pickle.dumps(pdu, pickle.HIGHEST_PROTOCOL)

    @ConnectedPB
    def version_release(self):
        """
        Retrieve the release version of the Jasmin instance.
        """
        return self.pb.callRemote('version_release')

    @ConnectedPB
    def version(self):
        """
        Retrieve the current Jasmin version.
        """
        return self.pb.callRemote('version')

    @ConnectedPB
    def list_bound_systemids(self):
        """
        Return a list of currently bound SMPP system IDs.
        """
        return self.pb.callRemote('list_bound_systemids')

    @ConnectedPB
    def deliverer_send_request(self, system_id, pdu):
        """
        Send a deliverer request for a given system_id using the provided PDU.
        The PDU is serialized before sending.
        """
        serialized_pdu = self._serialize_pdu(pdu)
        return self.pb.callRemote('deliverer_send_request', system_id, serialized_pdu)
