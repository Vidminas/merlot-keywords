from typing import Literal, Optional
from datetime import datetime
from dateutil.parser import isoparse
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from marshmallow import fields


@dataclass_json
@dataclass(frozen=True)
class MERLOTComment:
    url: str
    count: int
    avgscore: float


@dataclass_json
@dataclass(frozen=True)
class MERLOTLink:
    url: str
    count: int


@dataclass_json
@dataclass(frozen=True)
class MERLOTMaterial:
    url: str  # This is the URL of the material.
    detailURL: str  # This is the URL of the material detail page within MERLOT that describes this material and has links to create comments, learning exercises, etc.
    title: str  # This is the title of the material.
    authorName: str  # This contains the name of the author of the material or is empty if there is no author name.
    authorOrg: str  # Contains the organization of the author, if any. Note, if there is no authorName, but there is an authorOrg, the authorOrg will be returned in the authorName and the authorOrg will be empty.
    description: str  # This contains the description of the material. Note that the description can contain HTML elements.
    materialType: str  # This contains the material type as a string.
    keywords: str  # The keywords associated with this material.
    comments: Optional[
        list[MERLOTComment]
    ]  # For XML, this contains the URL to view the comments for this material in MERLOT. The tag also has two attributes, count, which is the number of comments, and avgscore, which is the average score given by users. This average score can range from zero to five and is given to a tenth of decimal place. A score of zero means that no score was given. If there are no comments, this tag will be empty with no attributes. For JSON, this is an object with three fields, url, count, and avgscore which correspond to the same items in the XML described above.
    bookmarkCollections: Optional[
        MERLOTLink
    ]  # For XML, this contains a the URL to view the bookmark collections that contain this material in MERLOT. The tag can also have an attribute of count that is the number of bookmark collections that contain this material. If there are no bookmark collections that contain this material, this tag will be empty with no attributes. For JSON, this is an object with two fields, url and count which correspond to the same items in the XML described above.
    coursePortfolios: Optional[
        MERLOTLink
    ]  # For XML, this contains a CDATA element which contains the URL to view the Course ePortfolios that contain this material in MERLOT. The tag can also have an attribute of count that is the number of Course ePortfolios that contain this material. If there are no Course ePortfolios that contain this material, this tag will be empty with no attributes. For JSON, this is an object with two fields, url and count which correspond to the same items in the XML described above.
    learningexercises: Optional[
        MERLOTLink
    ]  # For XML, this contains a CDATA element which contains the URL to view the learning exercises for this material in MERLOT. The tag can also have an attribute of count which is the number of learning exercises for this material. If there are no learning exercises for this material, this tag will be empty with no attributes. For JSON, this is an object with two fields, url and count which correspond to the same items in the XML described above.
    audiences: list[
        str
    ]  # For XML, if any intended audiences have been defined for this material, this element will contain the audience elements which hold the actual names of the audiences. There can be many intended audiences. For JSON, this will be an array audience names, if there are any audiences defined.
    languages: list[
        str
    ]  # For XML, this element contains the language elements which represent the language(s) for this material. For JSON, this will be an array three letter language codes.
    cefr: Optional[
        str
    ]  # If there is one, this is the CEFR standard this material complies with. See the cefr parameter above for more information.
    actfl: Optional[
        str
    ]  # If there is one, this is the ACTFL standard this material complies with. The ACTFL standard is mapped from the CEFR standard. See the cefr parameter above for more information
    creationDate: datetime = field(
        metadata=config(
            encoder=datetime.isoformat,
            decoder=isoparse,
            mm_field=fields.AwareDateTime(format="iso"),
        )
    )  # The date the material was initially added to MERLOT. For XML, this date is given in the format of "<3-Letter Month Abbreviation> <Date>, <Year>". For JSON, this is the number of milliseconds since Jan 1, 1970 which can be used in Javascript to create a Date object. This is the standard way of passing date information in JSON. See https://www.w3schools.com/js/js_dates.asp if you need more information on doing this.
    modifiedDate: datetime = field(
        metadata=config(
            encoder=datetime.isoformat,
            decoder=isoparse,
            mm_field=fields.AwareDateTime(format="iso"),
        )
    )  # The date the material's record was last modified in MERLOT. For XML, this date is given in the American format of "<3-Letter Month Abbreviation > <Date>, <Year>". For JSON, this is the number of milliseconds since Jan 1, 1970 which can be used in Javascript to create a Date object. This is the standard way of passing date information in JSON. See https://www.w3schools.com/js/js_dates.asp if you need more information on doing this.
    technicalFormat: Optional[
        str
    ]  # This has the technical format of the material. There can be more than one format which will be separated by commas. MERLOT has a list of standard formats that can be chosen by the submitter along with the ability to choose other and type anything, so this field can contain a variety of items.
    technicalrequirements: Optional[
        str
    ]  # This contain the technical requirements for the material. Note that the technical requirements can contain HTML elements.
    categories: list[
        object
    ]  # For XML, this element will contain the category elements which hold the actual categories. There can be many categories for a material. For JSON, this is an array of objects which hold the category information. Each of these objects will have two fields: path, which is the path name of the category, and url, which is the URL to search in MERLOT for materials within the category.
    cost: Literal[
        "yes", "no", "unsure"
    ]  # This contains either yes, no or unsure which describes if there is a cost associated with this material.
    creativecommons: str  # This contains unsure if the submitter was unsure if the items is under a Creative Commons License, no if the material is not under a Creative Commons License or the Creative Commons description string if it is under a Creative Commons License. This description string will be something like "by-nc-sa", or, if the license if a Creative Commons Zero license, the string will be "CC0".  For more information on the Creative Commons License, please visit http://www.creativecommons.org/
    compliant: Literal[
        "yes", "no", "unsure"
    ]  # This contains yes, no or unsure which describes if this material is Section 508 compliance (follows US rules for web accessibility).
    materialid: int  # This contains the id of the material within MERLOT.
    sourceavailable: Literal[
        "yes", "no", "unsure"
    ]  # This contains yes, no or unsure which describes if the source code is available for this material.
    merlotclassic: Optional[
        Literal[""]
    ]  # For XML, if present, indicates that this material is a recipient of the "MERLOT Classic" award. For JSON, if it is present and the value is empty quotes (""), then the material is a recipient of the "MERLOT Classic" award. If the value is null, the material is not a recipient of the "MERLOT Classic" award.*
    editorschoice: Optional[
        Literal[""]
    ]  # For XML, if present, indicates that this material is a recipient of the "Editors' Choice" award. For JSON, if it is present and the value is empty quotes (""), then the material is a recipient of the "Editors' Choice" award. If the value is null, the material is not a recipient of the "Editors' Choice" award.*


URLOKType = bool
URLErrorMessage = str
UrlReturnType = tuple[MERLOTMaterial, URLOKType, URLErrorMessage]
FILETYPES = Literal["PDF", "Unsure"]
