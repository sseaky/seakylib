#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2020/3/25 15:01

MIB = {
    'general':
        {
            'sysName': '1.3.6.1.2.1.1.5.0',
            'sysDescr': '1.3.6.1.2.1.1.1.0',
            'ifNumber': '1.3.6.1.2.1.2.1.0',
            'dot1dBasePortIfIndex': '1.3.6.1.2.1.17.1.4.1.2',
            'ifName': '1.3.6.1.2.1.31.1.1.1.1',
            'ifDescr': '1.3.6.1.2.1.2.2.1.2',
            'ifAlias': '1.3.6.1.2.1.31.1.1.1.18',
            'ifHCInOctets': '1.3.6.1.2.1.31.1.1.1.6',
            'ifHCOutOctets': '1.3.6.1.2.1.31.1.1.1.10',
            'ifHighSpeed': '1.3.6.1.2.1.31.1.1.1.15',
            'ifOperStatus': '1.3.6.1.2.1.2.2.1.8',
            'ipNetToMediaPhysAddress': '1.3.6.1.2.1.4.22.1.2',  # arp table
            'dot1qTpFdbPort': '1.3.6.1.2.1.17.7.1.2.2.1.2',  # mac-address table
            'lldpRemPortId': '1.0.8802.1.1.2.1.4.1.1.7',
            'lldpRemPortDesc': '1.0.8802.1.1.2.1.4.1.1.8',
            'lldpRemSysName': '1.0.8802.1.1.2.1.4.1.1.9',
            'lldpRemSysDesc': '1.0.8802.1.1.2.1.4.1.1.10',
            'dot1qVlanStaticEgressPorts': '1.3.6.1.2.1.17.7.1.4.3.1.2',  # comware, vrp
        },
    'h3c': {
        'dot3adAggPortSelectedAggID': '1.2.840.10006.300.43.1.2.1.1.12',
        'dot3adAggPortAttachedAggID': '1.2.840.10006.300.43.1.2.1.1.13',
        'hh3cStackMemberNum': '1.3.6.1.4.1.25506.2.91.1.2',
        'hh3cStackDomainId': '1.3.6.1.4.1.25506.2.91.1.8',
        'hh3cStackMemberID': '1.3.6.1.4.1.25506.2.91.2.1.1',
        'hh3cVlanInterfaceID': '1.3.6.1.4.1.25506.8.35.2.1.2.1.1',
        'hh3cdot1qVlanID': '1.3.6.1.4.1.25506.8.35.2.1.2.1.2',
        'hh3cIpAddrReadAddr': '1.3.6.1.4.1.25506.2.67.1.1.2.1.3',  # not exist
        'hh3cIpAddrReadMask': '1.3.6.1.4.1.25506.2.67.1.1.2.1.4',
        'hh3cdot1qVlanIpAddress': '1.3.6.1.4.1.25506.8.35.2.1.2.1.3',
        'hh3cdot1qVlanIpAddressMask': '1.3.6.1.4.1.25506.8.35.2.1.2.1.4',
        'Vlan2Addr': '1.3.6.1.4.1.25506.8.35.2.1.2.1.3.2',
        'Vlan2Mask': '1.3.6.1.4.1.25506.8.35.2.1.2.1.4.2',
        'hh3cLswSysIpAddr': '1.3.6.1.4.1.25506.8.35.18.1.1',
        'hh3cdot1qVlanPorts': '1.3.6.1.4.1.25506.8.35.2.1.1.1.3',
        'hh3cifVLANType': '1.3.6.1.4.1.25506.8.35.1.1.1.5',

        # 'hh3cLswSysVersion': '1.3.6.1.4.1.25506.8.35.18.1.4',
        # 'hh3cSysPackageVersion': '1.3.6.1.4.1.25506.2.3.1.7.2.1.10',
        # 'hh3cSysIpePackageVersion': '1.3.6.1.4.1.25506.2.3.1.8.3.1.7',
        'hh3cifVLANTrunkIndex': '1.3.6.1.4.1.25506.8.35.5.1.3.1.1',
        'hh3cifVLANTrunkPassListLow': '1.3.6.1.4.1.25506.8.35.5.1.3.1.4',
        'hh3cifVLANTrunkPassListHigh': '1.3.6.1.4.1.25506.8.35.5.1.3.1.5',
        'hh3cifVLANTrunkAllowListLow': '1.3.6.1.4.1.25506.8.35.5.1.3.1.6',
        'hh3cifVLANTrunkAllowListHigh': '1.3.6.1.4.1.25506.8.35.5.1.3.1.7',
        'hh3cAggLinkMode': '1.3.6.1.4.1.25506.8.25.1.1.1.3',  # 3-dynamic
        'hh3cAggLinkPortList': '1.3.6.1.4.1.25506.8.25.1.1.1.4',
        'hh3cAggPortListSelectedPorts': '1.3.6.1.4.1.25506.8.25.1.1.1.6',
    }
}
