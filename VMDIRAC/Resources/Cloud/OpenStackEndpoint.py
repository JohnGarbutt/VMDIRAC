""" OpenStackEndpoint class is the implementation of the OpenStack interface
    using the official python OpenStack SDK talking direct to OpenStack APIs
"""

import base64
import json
import os
import sys

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import makeGuid
import openstack
from openstack.config import loader

from VMDIRAC.Resources.Cloud.Endpoint import Endpoint

__RCSID__ = '$Id$'

class OpenStackEndpoint( Endpoint ):

  def __init__( self, parameters = {} ):
    super(OpenStackEndpoint, self).__init__(parameters=parameters)

    self.log = gLogger.getSubLogger( 'OpenStackEndpoint' )
    self.valid = False
    result = self.initialize()
    if result['OK']:
      self.log.debug( 'EC2Endpoint created and validated' )
      self.valid = True
    else:
      self.log.error( result['Message'] )

  def initialize( self ):

    # Defines the OpenStack Config cloud key in your config file,
    # typically in $HOME/.config/openstack/clouds.yaml
    # OS_CLIENT_CONFIG_FILE is used to customize the path
    # https://docs.openstack.org/openstacksdk/latest/user/config/configuration.html#config-clouds-yaml
    self._osCloud = self.parameters['OSCloud']

    try:
      self._conn = openstack.connect(cloud=self._osCloud)
      self._osConfig = loader.OpenStackConfig()
    except Exception, e:
      errorStatus = "Can't connect to OpenStack: " + str(e)
      return S_ERROR( errorStatus )

    result = self.__checkConnection()
    return result

  def __checkConnection( self ):
    """
    Checks connection status by trying to list the images.

    :return: S_OK | S_ERROR
    """
    try:
      self._conn.image.images()
    except Exception, e:
      return S_ERROR( e )

    return S_OK()

  def createInstances( self, vmsToSubmit ):
    outputDict = {}

    for nvm in xrange( vmsToSubmit ):
      self.log.debug( 'Creating VM %s/%s' )
      instanceID = makeGuid()[:8]
      result = self.createInstance( instanceID )
      if result['OK']:
        nodeid, nodeDict = result['Value']
        self.log.debug( 'Created VM instance %s/%s' % ( nodeid, instanceID ) )
        outputDict[nodeid] = nodeDict
      else:
        self.log.error( 'Create EC2 instance error:', result['Message'] )
        break

    return S_OK( outputDict )

  def createInstance( self, instanceID = '' ):
    if not instanceID:
      instanceID = makeGuid()[:8]

    self.parameters['VMUUID'] = instanceID
    self.parameters['VMType'] = self.parameters.get( 'CEType', 'OpenStack' )

    image = None
    if "ImageID" in self.parameters:
      image = self._conn.image.find_image(self.parameters["ImageID"])
    if image is None and 'ImageName' in self.parameters:
      image = self._conn.image.find_image(self.parameters["ImageName"])
    if image is None:
      return S_ERROR("Must specify a valid ImageID or ImageName.")

    if 'FlavorName' not in self.parameters:
      return S_ERROR( 'No flavor specified' )
    flavor = self._conn.compute.find_flavor(self.parameters['FlavorName'])
    if flavor is None:
      return S_ERROR("Flavor %s not found" % self.parameters['FlavorName'])

    keypair = None
    if 'KeyName' in self.parameters:
      keypair = self._conn.find_keypair(self.parameters['KeyName'])
      if keypair is None:
        return S_ERROR("Keypair %s not found" % self.parameters['KeyName'])

    result = self._createUserDataScript()
    if not result['OK']:
      return result
    userDataStr = str( result['Value'] )
    userdataB64str = base64.b64encode(userDataStr)

    network = None
    if "NetworkName" in self.parameters:
      networkName = self.parameters["NetworkName"]
      network = self._conn.network.find_network(networkName)
      if network is None:
        return S_ERROR("Unable to find network %s" % networkName)

    floatingIP = None
    if "PublicNetwork" in self.parameters:
      publicNetworkStr = self.parameters['PublicNetwork']
      publicNetwork = self._conn.network.find_network(publicNetworkStr)
      if not publicNetwork:
        return S_ERROR("Unable to find public network %s" % publicNetworkStr)

      floatingIP = self._conn.network.create_ip(
          floating_network_id = publicNetwork.id)
      floatingIP = self._conn.network.get_up(floatingIP)

    try:
      extras = {}
      if network is not None:
        extras['networks'] = [{"uuid": network.id}]

      server = conn.compute.create_server(
        name="DIRAC_%s" % instanceID, image_id=image.id, flavor_id=flavor.id,
        key_name=keypair.name, user_data=userdataB64str, **extras)
      self.log.debug("Waiting for server %s to build" % server.id)

      server = conn.compute.wait_for_server(server)
      self.log.debug("Server %s is built" % server.id)

    except Exception as e:
      errmsg = 'Exception in openstack create_instances: %s' % e
      self.log.error( errmsg )
      return S_ERROR( errmsg )

    if floatingIP:
      foundPort = None
      for port in self._conn.network.ports():
        if port.device_id == server.id:
          foundPort = port
          break
      self._conn.network.add_ip_to_port(foundPort, floatingIP)

    # Properties of the instance
    nodeDict = {}
    if floatingIP:
      nodeDict['PublicIP'] = floatingIP.floating_ip_address
    nodeDict['InstanceID'] = instanceID
    nodeDict['NumberOfCPUs'] = flavor.vcpus
    nodeDict['RAM'] = flavor.ram  # in MB
    nodeDict['DiskSize'] = flavor.disk

    return S_OK( ( server.id, nodeDict ) )

  def stopVM( self, nodeID, publicIP = '' ):
    """
    Given the node ID it gets the node details, which are used to destroy the
    node making use of the libcloud.openstack driver. If three is any public IP
    ( floating IP ) assigned, frees it as well.

    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )
      **public_ip** - `string`
        public IP assigned to the node if any

    :return: S_OK | S_ERROR
    """
    try:
      if publicIP:
        floatingIP = self._conn.network.find_ip(publicIP)
        if floatingIP is None:
          return S_ERROR( "unable to find floating ip %s" % publicIP)
        self._conn.network.remove_ip_from_port(floatingIP)
        self._conn.network.delete_ip(floatingIP)

      # TODO usure if nodeID = serveruuid or instanceID
      #   i.e. should we really look for DIRAC_%s % nodeID?
      server = self._conn.server.find_server(nodeID)
      if server is None:
        return S_ERROR("unable to find server: %s" % nodeID)

      self._conn.server.delete_server(server)
      # TODO above API is async so it could go into the error state
    except Exception as e:
      errmsg = 'Exception terminate instance %s: %s' % ( nodeID, e )
      self.log.error( errmsg )
      return S_ERROR( errmsg )

    return S_OK()
