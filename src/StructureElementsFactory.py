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
    def derive_new_class(cls, class_name: str, attributes: dict, base_class=BaseStructureClass):
        new_class = type(class_name, (base_class,), attributes)
        return new_class

    @classmethod
    def derive_and_init(cls, class_name: str, attributes: dict, lxml_element, base_class=BaseStructureClass):
        new_class = type(class_name, (base_class,), attributes)
        new_class()
        new_class.lxml_element = lxml_element
        return new_class
