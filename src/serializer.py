import json
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom


class BaseSerializer:
    @staticmethod
    def serialize(serializable):
        pass


class JSONSerializer(BaseSerializer):
    @staticmethod
    def serialize(serializable):
        pass


class XMLSerializer(BaseSerializer):
    @staticmethod
    def serialize(serializable):
        pass
