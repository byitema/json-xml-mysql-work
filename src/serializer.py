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
        rooms_dict = {'rooms': serializable}
        return json.dumps(rooms_dict, indent=4)


class XMLSerializer(BaseSerializer):
    @staticmethod
    def serialize(serializable):
        data = ET.Element('data')
        rooms_xml = ET.SubElement(data, 'rooms')
        for row in serializable:
            room_xml = ET.SubElement(rooms_xml, 'room')
            for key, value in row.items():
                element = ET.SubElement(room_xml, key)
                element.text = value.__str__()

        data_root = ET.ElementTree(data).getroot()
        return minidom.parseString(ET.tostring(data_root)).toprettyxml()
