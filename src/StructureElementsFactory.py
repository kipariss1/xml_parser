from dataclasses import dataclass


@dataclass
class BaseStructureClass:
    lxml_element = None

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)


class StructureElementsFactory:

    @classmethod
    def add_attributes(cls, upd_cls, attr: dict):
        for key, val in attr.items():
            setattr(upd_cls, key, val)

    @classmethod
    def derive_new_class(cls, class_name: str, attributes: dict, base_class=BaseStructureClass):
        new_class = type(class_name, (base_class,), attributes)
        return new_class

    @classmethod
    def derive_and_init(cls, class_name: str, attributes: dict, lxml_element, parent, base_class=BaseStructureClass):
        new_class = type(class_name, (base_class,), {key.lower(): val for key, val in attributes.items()})
        new_class()
        new_class.lxml_element = lxml_element
        setattr(new_class, 'parent', parent)
        return new_class
