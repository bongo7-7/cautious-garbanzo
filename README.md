Here are the full and complete code modifications for all the new and existing files required to add the "Wabunge" page to your project.

### Backend File Modifications (`backend_python`)

---
**1. New File: `backend_python/app/routes/wabunge.py`**

```python
# backend_python/app/routes/wabunge.py
from fastapi import APIRouter, Query, HTTPException, Path
from typing import Optional, List, Dict, Any
import logging
import json
import base64
from ..models import WabungeListResponse, ErrorResponse
from ..elasticsearch_client import es_client
from ..utils.helpers import build_es_query

router = APIRouter(prefix="/api", tags=["wabunge"])
logger = logging.getLogger(__name__)

def encode_search_after(sort_values: List) -> str:
    try:
        json_str = json.dumps(sort_values)
        return base64.b64encode(json_str.encode()).decode()
    except Exception as e:
        logger.error(f"Error encoding search_after: {e}")
        return ""

def decode_search_after(encoded: str) -> Optional[List]:
    try:
        if not encoded:
            return None
        json_str = base64.b64decode(encoded.encode()).decode()
        return json.loads(json_str)
    except Exception as e:
        logger.error(f"Error decoding search_after: {e}")
        return None

def transform_wabunge_for_list(doc: Dict[str, Any]) -> Dict[str, Any]:
    source = doc.get("_source", {})
    return {
        "id": source.get("id"),
        "name": source.get("name"),
        "title": source.get("title"),
        "phone1": source.get("phone1"),
        "phone2": source.get("phone2"),
        "chama": source.get("chama"),
        "address1": source.get("address1"),
        "address2": source.get("address2"),
        "highlight": doc.get("highlight", {})
    }

@router.get("/wabunge", response_model=WabungeListResponse)
async def list_wabunge(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None),
    wholeWord: bool = Query(False),
    wholeSentence: bool = Query(False),
    sortBy: str = Query("created_at"),
    sortOrder: str = Query("ASC"),
    searchAfter: Optional[str] = Query(None)
):
    try:
        query = build_es_query(
            search_term=search,
            search_fields=["name^3", "phone1", "phone2", "chama", "title"],
            whole_word=wholeWord,
            whole_sentence=wholeSentence
        )

        sort_field = "created_at"
        if sortBy == "name":
            sort_field = "name.keyword"
        
        sort_order = sortOrder.lower()
        sort_config = [
            {sort_field: {"order": sort_order, "missing": "_last"}},
            {"id": {"order": "asc"}}
        ]

        search_params = {
            "index": "wabunge",
            "query": query,
            "size": limit,
            "sort": sort_config,
            "track_total_hits": True,
            "highlight": {
                "fields": {
                    "name": {},
                    "phone1": {},
                    "phone2": {},
                    "chama": {},
                    "title": {}
                }
            }
        }

        if page > 1 and searchAfter:
            decoded_search_after = decode_search_after(searchAfter)
            if decoded_search_after:
                search_params["search_after"] = decoded_search_after
        else:
            search_params["from_"] = (page - 1) * limit

        result = await es_client.async_client.search(**search_params)

        wabunge_list = [transform_wabunge_for_list(hit) for hit in result["hits"]["hits"]]
        total_count = result["hits"]["total"]["value"]
        total_pages = (total_count + limit - 1) // limit if limit > 0 else 0

        next_search_after = None
        if wabunge_list and "sort" in result["hits"]["hits"][-1]:
            next_search_after = encode_search_after(result["hits"]["hits"][-1]["sort"])

        return {
            "success": True,
            "wabunge": wabunge_list,
            "pagination": {
                "currentPage": page,
                "totalPages": total_pages,
                "totalCount": total_count,
                "limit": limit,
                "searchAfter": next_search_after,
                "hasMore": len(wabunge_list) == limit
            }
        }

    except Exception as e:
        logger.error(f"Error fetching Wabunge data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
```

---
**2. Modified File: `backend_python/app/models.py`**

```python
# backend_python/app/models.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

# --- Common Models ---
class PaginationResponse(BaseModel):
    current_page: int = Field(..., alias="currentPage")
    total_pages: int = Field(..., alias="totalPages")
    total_count: int = Field(..., alias="totalCount")
    limit: int
    search_after: Optional[str] = Field(None, alias="searchAfter")
    has_more: bool = Field(..., alias="hasMore")

class ErrorResponse(BaseModel):
    success: bool = False
    message: str

# --- Company Models ---
class CompanyInfo(BaseModel):
    company_name: str = Field(..., alias="companyName")
    tin: Optional[str] = None
    authorised_share: Optional[str] = Field(None, alias="authorisedShare")
    incorporation_number: Optional[str] = Field(None, alias="incorporationNumber")

class ApplicantInfo(BaseModel):
    approval_status: str = Field(..., alias="approvalStatus")
    time_application_created: str = Field("", alias="timeApplicationCreated")

class CompanyListItem(BaseModel):
    id: str
    registration_type: str = Field("company", alias="registrationType")
    company_info_obj: CompanyInfo = Field(..., alias="companyInfoObj")
    applicant_obj: ApplicantInfo = Field(..., alias="applicantObj")
    highlight: Optional[Dict[str, List[str]]] = None

class CompaniesListResponse(BaseModel):
    success: bool = True
    companies: List[CompanyListItem]
    pagination: PaginationResponse

class CompanyDetailResponse(BaseModel):
    success: bool = True
    company: Dict[str, Any]

# --- Business Name Models ---
class BusinessNameInfo(BaseModel):
    business_name: str = Field(..., alias="businessName")
    registration_number: Optional[str] = Field(None, alias="registrationNumber")
    business_type: Optional[str] = Field(None, alias="businessType")

class BusinessNameListItem(BaseModel):
    id: str
    registration_type: str = Field("business_name", alias="registrationType")
    business_info_obj: BusinessNameInfo = Field(..., alias="businessInfoObj")
    applicant_obj: ApplicantInfo = Field(..., alias="applicantObj")
    highlight: Optional[Dict[str, List[str]]] = None

class BusinessNamesListResponse(BaseModel):
    success: bool = True
    business_names: List[BusinessNameListItem]
    pagination: PaginationResponse

class BusinessNameDetailResponse(BaseModel):
    success: bool = True
    business_name: Dict[str, Any]

# --- Corporate Shareholder Models ---
class CorporateShareholderListItem(BaseModel):
    id: str
    name: str
    country: Optional[str] = None
    shareholding_count: int = Field(..., alias="shareholdingCount")
    highlight: Optional[Dict[str, List[str]]] = None

class CorporateShareholdersListResponse(BaseModel):
    success: bool = True
    corporate_shareholders: List[CorporateShareholderListItem] = Field(..., alias="corporateShareholders")
    pagination: PaginationResponse

class Shareholding(BaseModel):
    company_id: str = Field(..., alias="companyId")
    company_name: str = Field(..., alias="companyName")
    shares_class: str = Field(..., alias="sharesClass")
    shares_count: int = Field(..., alias="sharesCount")
    approval_status: Optional[str] = Field(None, alias="approvalStatus")

class CorporateShareholderDetail(BaseModel):
    id: str
    name: str
    country: Optional[str] = None
    email_address: Optional[str] = Field(None, alias="emailAddress")
    phone_number: Optional[str] = Field(None, alias="phoneNumber")
    shareholdings: List[Shareholding]

class CorporateShareholderDetailResponse(BaseModel):
    success: bool = True
    corporate_shareholder: CorporateShareholderDetail = Field(..., alias="corporateShareholder")

# --- Unified People & Search Models ---
class PersonListItem(BaseModel):
    id: str
    full_name: str = Field(..., alias="fullName")
    date_of_birth: Optional[str] = Field(None, alias="dateOfBirth")
    nationality: Optional[str] = None
    role: str
    registration_id: str = Field(..., alias="registrationId")
    source_record_id: str = Field(..., alias="sourceRecordId")
    registration_name: str = Field(..., alias="registrationName")
    registration_type: str = Field(..., alias="registrationType")
    highlight: Optional[Dict[str, List[str]]] = None

class PeopleListResponse(BaseModel):
    success: bool = True
    people: List[PersonListItem]
    pagination: PaginationResponse

class PersonDetails(BaseModel):
    full_name: str = Field(..., alias="fullName")
    date_of_birth: Optional[str] = Field(None, alias="dateOfBirth")
    nationality: Optional[str] = None

class PersonRole(BaseModel):
    role: str # Can be comma-separated list of roles
    registration_id: str = Field(..., alias="registrationId")
    source_record_id: str = Field(..., alias="sourceRecordId")
    registration_name: str = Field(..., alias="registrationName")
    registration_type: str = Field(..., alias="registrationType")
    approval_status: Optional[str] = Field(None, alias="approvalStatus")

class ShareholdingInfo(BaseModel):
    company_id: str = Field(..., alias="companyId")
    company_name: str = Field(..., alias="companyName")
    share_value: float = Field(..., alias="shareValue")

class AssociatedTin(BaseModel):
    id: str
    tin_no: str = Field(..., alias="tinNo")
    name: Optional[str] = None
    tin_type: Optional[str] = Field(None, alias="tinType")
    postal_city: Optional[str] = Field(None, alias="postalCity")
    business_start_date: Optional[str] = Field(None, alias="businessStartDate")
    telephones: List[str] = []

# This uses a workaround for Pydantic v2 to allow list types for specific fields
class PersonProfileDetails(BaseModel):
    emailAddress: Optional[List[str]] = None
    mobilePhoneNumber: Optional[List[str]] = None
    class Config:
        extra = 'allow'

class FullPersonProfile(BaseModel):
    details: PersonDetails
    roles: List[PersonRole]
    detailed_info: Optional[PersonProfileDetails] = Field(None, alias="detailedInfo")
    shareholdings: Optional[List[ShareholdingInfo]] = None
    associated_tins: Optional[List[AssociatedTin]] = Field(None, alias="associatedTins")

class PersonDetailResponse(BaseModel):
    success: bool = True
    person: FullPersonProfile

class CompanyContactResult(BaseModel):
    company_id: str = Field(..., alias="companyId")
    company_name: str = Field(..., alias="companyName")
    found_in_sections: str = Field(..., alias="foundInSections")
    registration_type: str = Field("company", alias="registrationType")
    approval_status: Optional[str] = Field(None, alias="approvalStatus")

class BusinessNameContactResult(BaseModel):
    business_name_id: str = Field(..., alias="businessNameId")
    business_name: str = Field(..., alias="businessName")
    found_in_sections: str = Field(..., alias="foundInSections")
    registration_type: str = Field("business_name", alias="registrationType")
    approval_status: Optional[str] = Field(None, alias="approvalStatus")

class PersonContactResult(BaseModel):
    person_id: str = Field(..., alias="personId")
    full_name: str = Field(..., alias="fullName")
    role: str
    registration_id: str = Field(..., alias="registrationId")
    registration_name: str = Field(..., alias="registrationName")
    registration_type: str = Field(..., alias="registrationType")
    approval_status: Optional[str] = Field(None, alias="approvalStatus")

class ContactSearchResponse(BaseModel):
    success: bool = True
    companies: List[CompanyContactResult]
    business_names: List[BusinessNameContactResult]
    people: List[PersonContactResult]
    associated_tins: List[AssociatedTin] = Field([], alias="associatedTins")

# --- Network Graph Models ---
class GraphNode(BaseModel):
    id: str
    name: str
    type: str # 'person', 'company', 'business-name', or 'corporate-shareholder'
    val: Optional[float] = 1 # for node size
    most_common_role: Optional[str] = Field(None, alias="mostCommonRole")
    date_of_birth: Optional[str] = Field(None, alias="dateOfBirth")
    gender: Optional[str] = None

class GraphLink(BaseModel):
    source: str
    target: str
    roles: List[str]

class GraphData(BaseModel):
    nodes: List[GraphNode]
    links: List[GraphLink]

class GraphDataResponse(BaseModel):
    success: bool = True
    graph: GraphData
    message: Optional[str] = None

# --- Key Influencers Models ---
class LeaderboardPerson(BaseModel):
    id: str
    name: str
    value: float
    nationality: Optional[str] = None

class LeaderboardResponse(BaseModel):
    success: bool = True
    leaderboard: List[LeaderboardPerson]
    pagination: PaginationResponse

class CoDirectorMatrixEntry(BaseModel):
    person1_id: str
    person1_name: str
    person2_id: str
    person2_name: str
    count: int

class CoDirectorMatrixResponse(BaseModel):
    success: bool = True
    matrix: List[CoDirectorMatrixEntry]
    people: List[Dict[str, str]]

class PathNode(BaseModel):
    id: str
    name: str
    type: str

class PathLink(BaseModel):
    source: str
    target: str
    label: str

class SixDegreesResponse(BaseModel):
    success: bool = True
    path_found: bool
    message: str
    nodes: Optional[List[PathNode]] = None
    links: Optional[List[PathLink]] = None

# --- Phone Filter Models ---
class PhoneFilterPerson(BaseModel):
    id: str
    full_name: str = Field(..., alias="fullName")
    age: Optional[int] = None
    mobile_phone_number: Optional[str] = Field(None, alias="mobilePhoneNumber")
    nationality: Optional[str] = None
    formatted_number: Optional[str] = Field(..., alias="formattedNumber")
    standard_number: Optional[str] = Field(..., alias="standardNumber")
    highlight: Optional[Dict[str, List[str]]] = None

class PhoneFilterListResponse(BaseModel):
    success: bool = True
    people: List[PhoneFilterPerson]
    pagination: PaginationResponse

# --- NEW: Contact Exporter Models ---
class ContactExportRequest(BaseModel):
    nationality: str = "Tanzanian"
    min_age: int = 25
    max_age: int = 55
    sample_size: int = 5500
    name_prefix: str = "wapp_logo_CM21_"
    file_name: str = "contacts.csv"

class ContactExportPreviewStats(BaseModel):
    initial_count: int
    duplicates_removed: int
    engaged_removed: int
    final_count: int
    sample_size: int

class ContactExportPreviewResponse(BaseModel):
    success: bool = True
    stats: ContactExportPreviewStats
    sample_data_head: List[Dict[str, str]]
    sample_data_tail: List[Dict[str, str]]

# --- TRA TINS Models ---
class TraTinListItem(BaseModel):
    id: str
    tin_no: str = Field(..., alias="tinNo")
    name: Optional[str] = None
    tin_type: Optional[str] = Field(None, alias="tinType")
    postal_city: Optional[str] = Field(None, alias="postalCity")
    business_start_date: Optional[str] = Field(None, alias="businessStartDate")
    telephones: List[str] = []
    age: Optional[int] = None
    highlight: Optional[Dict[str, List[str]]] = None

class TraTinsListResponse(BaseModel):
    success: bool = True
    tins: List[TraTinListItem]
    pagination: PaginationResponse

class TraTinDetailResponse(BaseModel):
    success: bool = True
    tin: Dict[str, Any]

# --- Wabunge Models ---
class WabungeListItem(BaseModel):
    id: str
    name: str
    title: Optional[str] = None
    phone1: Optional[str] = None
    phone2: Optional[str] = None
    chama: Optional[str] = None
    address1: Optional[str] = None
    address2: Optional[str] = None
    highlight: Optional[Dict[str, List[str]]] = None

class WabungeListResponse(BaseModel):
    success: bool = True
    wabunge: List[WabungeListItem]
    pagination: PaginationResponse
```

---
**3. Modified File: `backend_python/app/elasticsearch_client.py`**

```python
# backend_python/app/elasticsearch_client.py
from elasticsearch import Elasticsearch, AsyncElasticsearch
from elasticsearch.helpers import bulk, async_bulk
from typing import Dict, Any, List, Optional
import logging
from .config import settings
import json

logger = logging.getLogger(__name__)

class ElasticsearchClient:
    """Elasticsearch client wrapper with connection management"""

    def __init__(self):
        self.client: Optional[Elasticsearch] = None
        self.async_client: Optional[AsyncElasticsearch] = None

    def connect(self):
        """Establish synchronous connection to Elasticsearch"""
        try:
            self.client = Elasticsearch(
                [settings.elasticsearch_url],
                basic_auth=(settings.elasticsearch_user, settings.elasticsearch_password),
                verify_certs=False,
                ssl_show_warn=False,
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            if self.client.ping():
                logger.info(f"Connected to Elasticsearch at {settings.elasticsearch_url}")
            else:
                raise ConnectionError("Cannot connect to Elasticsearch")
        except Exception as e:
            logger.error(f"Elasticsearch connection error: {str(e)}")
            raise

    def connect_async(self):
        """Establish asynchronous connection to Elasticsearch"""
        try:
            self.async_client = AsyncElasticsearch(
                [settings.elasticsearch_url],
                basic_auth=(settings.elasticsearch_user, settings.elasticsearch_password),
                verify_certs=False,
                ssl_show_warn=False,
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            logger.info(f"Async client configured for Elasticsearch at {settings.elasticsearch_url}")
        except Exception as e:
            logger.error(f"Elasticsearch async connection error: {str(e)}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info("Elasticsearch connection closed")

    async def close_async(self):
        if self.async_client:
            await self.async_client.close()
            logger.info("Elasticsearch async connection closed")

    def create_indices(self):
        """Create Elasticsearch indices with mappings"""
        if not self.client:
            raise ConnectionError("Synchronous client not connected.")

        # --- Define reusable property mappings ---
        applicant_properties = {
            "emailAddress": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "mobilePhoneNumber": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "mobilePhoneNumber_normalized": {"type": "keyword"},
            "approvalStatus": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "nationalID": {"type": "keyword"},
            "passportNumber": {"type": "keyword"},
            "nida_region": {"type": "keyword"},
            "nida_district": {"type": "keyword"},
            "nida_ward": {"type": "keyword"}
        }

        person_nested_properties = {
            "emailAddress": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "mobilePhoneNumber": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "mobilePhoneNumber_normalized": {"type": "keyword"},
            "nationalId": {"type": "keyword"},
            "nationalID": {"type": "keyword"},
            "passportNumber": {"type": "keyword"},
            "shareholderId": {"type": "keyword"},
            "type": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "country": {"type": "keyword"},
            "lastName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "firstName": {"type": "text"},
            "middleName": {"type": "text"},
            "dateOfBirth": {"type": "keyword"},
            "nationality": {"type": "keyword"},
            "nida_region": {"type": "keyword"},
            "nida_district": {"type": "keyword"},
            "nida_ward": {"type": "keyword"},
            "shares_count": {"type": "long"},
            "share_value": {"type": "double"}
        }

        autocomplete_analyzer = {
            "analysis": {
                "filter": {
                    "autocomplete_filter": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 20
                    }
                },
                "analyzer": {
                    "autocomplete_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "autocomplete_filter"
                        ]
                    }
                }
            }
        }
        
        # --- Company Index Mapping ---
        companies_mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "company_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}, "suggest": {"type": "completion"}, "autocomplete": {"type": "text", "analyzer": "autocomplete_analyzer", "search_analyzer": "standard"}}},
                    "registration_type": {"type": "keyword"},
                    "incorporation_number": {"type": "keyword"},
                    "tin": {"type": "keyword"},
                    "incorporation_date": {"type": "keyword"},
                    "applicant_info": {"type": "object", "properties": applicant_properties},
                    "business_place": {"type": "object", "enabled": False},
                    "company_secretary_detail": {"type": "object", "properties": person_nested_properties},
                    "other_info": {"type": "object", "enabled": False},
                    "director_details": {"type": "nested", "properties": person_nested_properties},
                    "shareholder_details": {"type": "nested", "properties": person_nested_properties},
                    "business_activities": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "authorised_share": {"type": "long"},
                    "time_application_created": {"type": "keyword"},
                    "time_application_submitted": {"type": "keyword"},
                    "service_type": {"type": "keyword"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.mapping.total_fields.limit": 2000,
                "index.max_result_window": 3000000,
                **autocomplete_analyzer
            }
        }
        
        # --- Business Name Index Mapping ---
        business_names_mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "business_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}, "suggest": {"type": "completion"}, "autocomplete": {"type": "text", "analyzer": "autocomplete_analyzer", "search_analyzer": "standard"}}},
                    "registration_type": {"type": "keyword"},
                    "registration_number": {"type": "keyword"},
                    "registration_date": {"type": "keyword"},
                    "business_type": {"type": "keyword"},
                    "applicant_info": {"type": "object", "properties": applicant_properties},
                    "business_place": {"type": "object", "enabled": False},
                    "owner_details": {"type": "nested", "properties": person_nested_properties},
                    "persons_who_can_update_details": {"type": "nested", "properties": person_nested_properties},
                    "bank_operator_details": {"type": "nested", "properties": person_nested_properties},
                    "business_activities": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "time_application_created": {"type": "keyword"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.mapping.total_fields.limit": 2000,
                "index.max_result_window": 3000000,
                **autocomplete_analyzer
            }
        }
        
        # --- People Index Mapping (Unified) ---
        people_mapping = {
            "mappings": {
                "properties": {
                    "person_identifier": {"type": "keyword"},
                    "full_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}, "suggest": {"type": "completion"}, "autocomplete": {"type": "text", "analyzer": "autocomplete_analyzer", "search_analyzer": "standard"}}},
                    "date_of_birth": {"type": "date", "format": "yyyy-MM-dd||dd/MM/yyyy||epoch_millis"},
                    "nationality": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "role": {"type": "keyword"},
                    "registration_id": {"type": "keyword"},
                    "source_record_id": {"type": "keyword"},
                    "registration_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "registration_type": {"type": "keyword"},
                    "approval_status": {"type": "keyword"},
                    "record_created_at": {"type": "date", "format": "dd/MM/yyyy HH:mm:ss"},
                    "details": {"type": "object", "properties": person_nested_properties}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.mapping.total_fields.limit": 2000,
                "index.max_result_window": 3000000,
                **autocomplete_analyzer
            }
        }

        # --- NEW: Corporate Shareholder Index Mapping ---
        corporate_shareholders_mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}, "autocomplete": {"type": "text", "analyzer": "autocomplete_analyzer", "search_analyzer": "standard"}}},
                    "country": {"type": "keyword"},
                    "email_address": {"type": "keyword"},
                    "phone_number": {"type": "keyword"},
                    "phone_number_normalized": {"type": "keyword"},
                    "shareholdings": {
                        "type": "nested",
                        "properties": {
                            "company_id": {"type": "keyword"},
                            "company_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "shares_class": {"type": "keyword"},
                            "shares_count": {"type": "long"}
                        }
                    },
                    "shareholding_count": {"type": "integer"},
                    "created_at": {"type": "date"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.mapping.total_fields.limit": 2000,
                "index.max_result_window": 3000000,
                **autocomplete_analyzer
            }
        }
        
        # --- NEW: TRA TINS Index Mapping ---
        tra_tins_mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "tin_no": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "autocomplete": {"type": "text", "analyzer": "autocomplete_analyzer", "search_analyzer": "standard"}
                        }
                    },
                    "firstname": {"type": "text"},
                    "middlename": {"type": "text"},
                    "lastname": {"type": "text"},
                    "tin_type": {"type": "keyword"},
                    "national_id": {"type": "keyword"},
                    "incorporation_number": {"type": "keyword"},
                    "telephones": {"type": "keyword"},
                    "telephones_normalized": {"type": "keyword"},
                    "business_start_date": {"type": "date", "format": "M/d/yyyy H:mm:ss a||yyyy-MM-dd||epoch_millis", "ignore_malformed": True},
                    "pobox": {"type": "keyword"},
                    "postal_city": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "date_of_birth": {"type": "date", "format": "yyyy-MM-dd", "ignore_malformed": True},
                    "age": {"type": "integer"},
                    "nida_region": {"type": "keyword"},
                    "nida_district": {"type": "keyword"},
                    "nida_ward": {"type": "keyword"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.max_result_window": 3000000,
                **autocomplete_analyzer
            }
        }
        
        # --- NEW: Wabunge Index Mapping ---
        wabunge_mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "autocomplete": {"type": "text", "analyzer": "autocomplete_analyzer", "search_analyzer": "standard"}
                        }
                    },
                    "title": {"type": "keyword"},
                    "phone1": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "phone1_normalized": {"type": "keyword"},
                    "phone2": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "phone2_normalized": {"type": "keyword"},
                    "chama": {"type": "keyword"},
                    "address1": {"type": "text"},
                    "address2": {"type": "text"},
                    "created_at": {"type": "date"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.max_result_window": 10000,
                **autocomplete_analyzer
            }
        }

        indices_to_create = {
            "companies": companies_mapping,
            "business_names": business_names_mapping,
            "people": people_mapping,
            "corporate_shareholders": corporate_shareholders_mapping,
            "tra_tins": tra_tins_mapping,
            "wabunge": wabunge_mapping
        }

        for index_name, mapping in indices_to_create.items():
            if self.client.indices.exists(index=index_name):
                self.client.indices.delete(index=index_name)
                logger.info(f"Deleted existing '{index_name}' index.")
            self.client.indices.create(index=index_name, body=mapping)
            logger.info(f"Created '{index_name}' index")

es_client = ElasticsearchClient()
```

---
**4. Modified File: `backend_python/migration/utils.py`**

```python
# backend_python/migration/utils.py
import logging
from pathlib import Path
import json
import csv
from typing import Dict, Any, Generator

# Add project root to path to allow importing app modules
# This is necessary if you ever run this file directly for testing
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.utils.helpers import normalize_phone_number
from tqdm import tqdm

logger = logging.getLogger(__name__)

def get_nida_location(national_id: str, postcode_data: Dict) -> Dict:
    """Extracts NIDA location details based on the postcode in a national ID."""
    if not national_id or len(national_id) < 13:
        return {}
    postcode = national_id[8:13]
    location = postcode_data.get(postcode)
    if location:
        return {
            "nida_region": location.get("region"),
            "nida_district": location.get("district"),
            "nida_ward": location.get("ward")
        }
    return {}

def format_duration(seconds: float) -> str:
    """Formats a duration in seconds into a human-readable HH:MM:SS string."""
    s = int(seconds)
    hours, remainder = divmod(s, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def clean_person_details(person_data: dict, postcode_data: Dict) -> dict:
    """Cleans and standardizes a person's data dictionary."""
    if not isinstance(person_data, dict):
        return {}
    cleaned_data = person_data.copy()
    # Clean booleans
    for bool_field in ['genderMale', 'genderFemale']:
        value = cleaned_data.get(bool_field)
        if isinstance(value, str) and value.lower() in ['noresult', '']:
            cleaned_data[bool_field] = False
        elif not isinstance(value, bool):
            cleaned_data[bool_field] = False

    # Normalize phone number
    phone_number = cleaned_data.get('mobilePhoneNumber')
    cleaned_data['mobilePhoneNumber_normalized'] = normalize_phone_number(phone_number)

    # Add NIDA location from National ID and normalize nationality
    national_id = cleaned_data.get('nationalID') or cleaned_data.get('nationalId')
    if national_id:
        location_info = get_nida_location(national_id, postcode_data)
        cleaned_data.update(location_info)
    
        # Check current nationality from both possible fields
        current_nationality = cleaned_data.get('nationality') or cleaned_data.get('nationalityApplicant')
        is_nationality_missing = not current_nationality or \
                                 (isinstance(current_nationality, str) and (current_nationality.strip().lower() in ['', 'noresult']))
        
        # If nationality is missing AND we found a valid NIDA location (non-empty dict), set to Tanzanian
        if is_nationality_missing and location_info:
            cleaned_data['nationality'] = "Tanzanian"
            # Also update the applicant-specific field if it's present in the dictionary for consistency
            if 'nationalityApplicant' in cleaned_data:
                cleaned_data['nationalityApplicant'] = "Tanzanian"

    return cleaned_data

def count_docs_in_json_dir(json_dir: str) -> int:
    """Memory-efficiently count the total number of documents in a directory of JSON files for tqdm."""
    json_path = Path(json_dir)
    if not json_path.is_dir():
        return 0
    total_docs = 0
    json_files = list(json_path.rglob("*.json")) # Changed from glob() to rglob()
    logger.info(f"Found {len(json_files)} JSON files in '{json_path.name}' and subdirectories. Counting documents...")
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            total_docs += len(data)
        except Exception as e:
            logger.warning(f"Could not count docs in {json_file.name}: {e}")
    return total_docs

def generate_es_actions(json_dir: str, index_name: str, clean_func, *args) -> Generator[Dict[str, Any], None, None]:
    """Yields Elasticsearch bulk actions from a directory of JSON files without loading all into memory."""
    json_path = Path(json_dir)
    if not json_path.is_dir():
        logger.warning(f"Directory not found: {json_dir}. Skipping.")
        return

    processed_ids = set()
    for json_file in list(json_path.rglob("*.json")): # Changed from glob() to rglob()
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            for doc_id, doc_data in data.items():
                if doc_id in processed_ids:
                    continue
                
                cleaned_doc = clean_func(doc_id, doc_data, *args)
                if cleaned_doc:
                    yield {
                        "_index": index_name,
                        "_id": cleaned_doc.get("id"),
                        "_source": cleaned_doc
                    }
                processed_ids.add(doc_id)
        except Exception as e:
            logger.error(f"Error yielding actions from file {json_file.name}: {e}")

def generate_people_actions(json_dir: str, clean_func, extract_func, *args) -> Generator[Dict[str, Any], None, None]:
    """Yields Elasticsearch bulk actions for the 'people' index from parent documents."""
    json_path = Path(json_dir)
    if not json_path.is_dir():
        return

    processed_parent_ids = set()
    for json_file in list(json_path.rglob("*.json")): # MODIFIED: Changed from glob() to rglob() for recursive search
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            for doc_id, doc_data in data.items():
                if doc_id in processed_parent_ids:
                    continue
                cleaned_parent_doc = clean_func(doc_id, doc_data, *args)
                if cleaned_parent_doc:
                    yield from extract_func(cleaned_parent_doc)
                processed_parent_ids.add(doc_id)
        except Exception as e:
            logger.error(f"Error yielding people actions from file {json_file.name}: {e}")

def count_people_in_json_dir(json_dir: str, clean_func, extract_func, *args) -> int:
    """Memory-efficiently count the total number of people documents that will be generated for a progress bar."""
    json_path = Path(json_dir)
    if not json_path.is_dir():
        return 0

    total_people = 0
    processed_parent_ids = set()
    json_files = list(json_path.glob("*.json"))
    for json_file in tqdm(json_files, desc=f"Counting people in {json_path.name}", unit="file", leave=False):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for doc_id, doc_data in data.items():
                if doc_id in processed_parent_ids:
                    continue
                cleaned_parent_doc = clean_func(doc_id, doc_data, *args)
                if cleaned_parent_doc:
                    count = sum(1 for _ in extract_func(cleaned_parent_doc))
                    total_people += count
                processed_parent_ids.add(doc_id)
        except Exception as e:
            logger.error(f"Error counting people from file {json_file.name}: {e}")
    return total_people

def count_docs_in_json_list_dir(json_dir: str) -> int:
    """Memory-efficiently count total objects in a directory of JSON list files."""
    json_path = Path(json_dir)
    if not json_path.is_dir():
        return 0
    total_docs = 0
    json_files = list(json_path.rglob("*.json"))
    logger.info(f"Found {len(json_files)} JSON files in '{json_path.name}'. Counting documents...")
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                total_docs += len([item for item in data if isinstance(item, dict)])
        except Exception as e:
            logger.warning(f"Could not count docs in list file {json_file.name}: {e}")
    return total_docs

def generate_es_actions_from_list(json_dir: str, index_name: str, clean_func, *args) -> Generator[Dict[str, Any], None, None]:
    """Yields Elasticsearch bulk actions from a directory of JSON files where each file contains a list of objects."""
    json_path = Path(json_dir)
    if not json_path.is_dir():
        logger.warning(f"Directory not found: {json_dir}. Skipping.")
        return

    processed_ids = set()
    for json_file in list(json_path.rglob("*.json")):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                logger.warning(f"Skipping {json_file.name}, expected a list but got {type(data)}.")
                continue

            for doc_data in data:
                if not isinstance(doc_data, dict):
                    continue # Skip nulls or other non-dict items
                
                cleaned_doc = clean_func(doc_data, *args)
                if cleaned_doc:
                    doc_id = cleaned_doc.get("id")
                    if doc_id and doc_id not in processed_ids:
                        yield {
                            "_index": index_name,
                            "_id": doc_id,
                            "_source": cleaned_doc
                        }
                        processed_ids.add(doc_id)
        except Exception as e:
            logger.error(f"Error yielding actions from list file {json_file.name}: {e}")


def count_rows_in_csv(csv_path: str) -> int:
    """Counts the number of data rows in a CSV file."""
    path = Path(csv_path)
    if not path.is_file():
        return 0
    try:
        with open(path, 'r', encoding='utf-8') as f:
            # -1 to account for header row
            return sum(1 for row in f) - 1
    except Exception as e:
        logger.error(f"Could not count rows in {path.name}: {e}")
        return 0


def generate_es_actions_from_csv(csv_path: str, index_name: str, clean_func, *args) -> Generator[Dict[str, Any], None, None]:
    """Yields Elasticsearch bulk actions from a CSV file."""
    path = Path(csv_path)
    if not path.is_file():
        logger.warning(f"CSV file not found: {csv_path}. Skipping.")
        return
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                cleaned_doc = clean_func(row, i, *args)
                if cleaned_doc:
                    yield {
                        "_index": index_name,
                        "_id": cleaned_doc.get("id"),
                        "_source": cleaned_doc
                    }
    except Exception as e:
        logger.error(f"Error yielding actions from CSV file {path.name}: {e}")

```

---
**5. Modified File: `backend_python/migration/processors.py`**

```python
# backend_python/migration/processors.py
import logging
import re
from datetime import datetime
from pathlib import Path
import json
from typing import Dict, Any, Generator, Optional, Tuple
from tqdm import tqdm
from .utils import clean_person_details, get_nida_location
from app.utils.helpers import format_date_for_es, normalize_phone_number

logger = logging.getLogger(__name__)

def _extract_dob_and_age_from_nida(national_id: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """Extracts DOB and calculates age from a NIDA number string."""
    if not national_id or not isinstance(national_id, str) or len(national_id) < 8:
        return None, None
    try:
        year = int(national_id[0:4])
        month = int(national_id[4:6])
        day = int(national_id[6:8])

        # Basic validation
        if not (1900 <= year <= datetime.now().year and 1 <= month <= 12 and 1 <= day <= 31):
            return None, None
            
        birth_date = datetime(year, month, day)
        today = datetime.now()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return birth_date.strftime('%Y-%m-%d'), age
    except (ValueError, TypeError):  # Handles cases where slicing or int conversion fails
        return None, None

def parse_shares_taken_html(html_content: str) -> Dict[str, Dict[str, Any]]:
    """Parses the HTML table of shares taken to extract shareholder IDs and share counts."""
    if not html_content or not isinstance(html_content, str):
        return {}
    share_details = {}
    # Regex to find table rows and capture cells
    rows = re.findall(r'<tr.*?>(.*?)<\/tr>', html_content, re.DOTALL)
    for row_html in rows:
        cells = re.findall(r'<td.*?>(.*?)<\/td>', row_html, re.DOTALL)
        if len(cells) >= 4:
            shareholder_id = cells[0].strip()
            # Clean up number span
            shares_str = re.sub(r'<.*?>', '', cells[3]).strip().replace(',', '')
            try:
                shares_count = int(shares_str)
                share_details[shareholder_id] = {
                    'shares_class': cells[2].strip(),
                    'shares_count': shares_count
                }
            except (ValueError, IndexError):
                continue
    return share_details

def aggregate_corporate_shareholders(company_json_dir: str) -> Dict[str, Any]:
    """Aggregates all corporate shareholder data by iterating through company files."""
    corporate_shareholders_agg = {}
    company_path = Path(company_json_dir)
    company_files = list(company_path.glob("*.json")) if company_path.is_dir() else []
    
    for json_file in tqdm(company_files, desc="Aggregating corporate shareholders", unit="file"):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for company_id, company_data in data.items():
            if not company_data:
                logger.warning(f"Skipping empty or null data for company ID {company_id} in {json_file.name}")
                continue

            other_sections = company_data.get("otherRemainingSections") or {}
            shares_taken_html = other_sections.get("sharesTaken", "")
            share_details = parse_shares_taken_html(shares_taken_html)
            
            shareholder_details_raw = company_data.get('shareholderDetail') or {}
            for key, shareholder in shareholder_details_raw.items():
                if shareholder and shareholder.get('type') == 'Other':  # 'Other' indicates a corporate entity
                    shareholder_id = shareholder.get('shareholderId')
                    if not shareholder_id:
                        continue

                    # Extract name from the dictionary key as a fallback
                    name_from_key = key.split(',', 1)[-1].strip() if ',' in key else key

                    if shareholder_id not in corporate_shareholders_agg:
                        phone = shareholder.get('mobilePhoneNumber')
                        corporate_shareholders_agg[shareholder_id] = {
                            'id': shareholder_id,
                            'name': name_from_key,
                            'country': shareholder.get('country'),
                            'email_address': shareholder.get('emailAddress'),
                            'phone_number': phone,
                            'phone_number_normalized': normalize_phone_number(phone),
                            'shareholdings': []
                        }
                    
                    share_info = share_details.get(shareholder_id, {})
                    corporate_shareholders_agg[shareholder_id]['shareholdings'].append({
                        'company_id': company_id,  # This is the record ID, which the detail page needs
                        'company_name': (company_data.get("companyInfoObj") or {}).get("companyName"),
                        'shares_class': share_info.get('shares_class', 'N/A'),
                        'shares_count': share_info.get('shares_count', 0)
                    })
    return corporate_shareholders_agg

# --- TRA TIN Processor ---
def clean_and_prepare_tra_tin(tin_data: dict, postcode_data: Dict) -> Optional[Dict]:
    """Cleans and prepares a single TRA TIN document."""
    if not isinstance(tin_data, dict) or not tin_data:
        return None

    # Use tin_No_Og as primary ID, fallback to incorporation_number
    tin_no = tin_data.get("tin_No_Og") or tin_data.get("incorporation_number")
    if not tin_no:
        return None # Skip records without a TIN

    # Combine and normalize telephones
    telephones_raw = [
        tin_data.get("telephone1"),
        tin_data.get("telephone2"),
        tin_data.get("telephone3")
    ]
    telephones = [str(tel) for tel in telephones_raw if tel and str(tel).strip()]
    telephones_normalized = list(set(
        norm_phone for phone in telephones if (norm_phone := normalize_phone_number(phone)) is not None
    ))
    
    # Handle date format
    business_start_date = tin_data.get("businessstartdate")

    # Extract DOB and age from national_id
    national_id = tin_data.get("national_id")
    date_of_birth, age = _extract_dob_and_age_from_nida(national_id)

    # Get NIDA location
    location_info = {}
    if national_id:
        location_info = get_nida_location(national_id, postcode_data)

    doc = {
        "id": tin_no,
        "tin_no": tin_no,
        "name": tin_data.get("name"),
        "firstname": tin_data.get("firstname"),
        "middlename": tin_data.get("middlename"),
        "lastname": tin_data.get("lastname"),
        "tin_type": tin_data.get("tin_type"),
        "national_id": national_id,
        "incorporation_number": tin_data.get("incorporation_number"),
        "telephones": telephones,
        "telephones_normalized": telephones_normalized,
        "business_start_date": business_start_date,
        "pobox": tin_data.get("pobox"),
        "postal_city": tin_data.get("postalcity"),
        "created_at": datetime.now().isoformat(),
        "date_of_birth": date_of_birth,
        "age": age
    }
    doc.update(location_info) # Add location fields
    return doc

# --- Wabunge Processor ---
def clean_and_prepare_wabunge(row: dict, row_num: int, *args) -> Optional[Dict]:
    """Cleans and prepares a single row from the wabunge CSV."""
    if not isinstance(row, dict) or not row.get("NAME"):
        return None

    name = row.get("NAME", "").strip()
    phone1 = row.get("PHONE1", "").strip()
    phone2 = row.get("PHONE2", "").strip()

    return {
        "id": f"wabunge-{row_num + 1}",
        "name": name,
        "title": row.get("TITLE", "").strip() or None,
        "phone1": phone1 or None,
        "phone1_normalized": normalize_phone_number(phone1),
        "phone2": phone2 or None,
        "phone2_normalized": normalize_phone_number(phone2),
        "chama": row.get("CHAMA", "").strip() or None,
        "address1": row.get("ADDRESS", "").strip() or None,
        "address2": row.get("ADDRES2", "").strip() or None,
        "created_at": datetime.now().isoformat()
    }

# --- Company Processors ---
def clean_and_prepare_company(company_id: str, data: dict, postcode_data: Dict) -> Dict:
    """Cleans and prepares a single company document for Elasticsearch indexing."""
    company_info = data.get('companyInfoObj') or {}
    applicant_info = data.get('applicantObj') or {}

    # Normalize approval status
    status = applicant_info.get('approvalStatus', 'Unknown')
    if status == 'noResult':
        status = 'Unknown'
    applicant_info['approvalStatus'] = status

    other_sections = data.get('otherRemainingSections') or {}
    shareholder_details_raw = data.get('shareholderDetail') or {}

    # --- Calculate share value logic ---
    authorised_share_str = str(other_sections.get('authorisedShare', '0')).replace(',', '')
    number_of_shares_str = str(other_sections.get('numberOfShares', '0')).replace(',', '')
    shares_taken_html = other_sections.get("sharesTaken", "")
    
    value_per_share = 0
    try:
        auth_share_val = float(authorised_share_str)
        num_shares_val = float(number_of_shares_str)
        if num_shares_val > 0:
            value_per_share = auth_share_val / num_shares_val
    except (ValueError, TypeError):
        pass
    
    parsed_shares_info = parse_shares_taken_html(shares_taken_html)
    
    company_name = company_info.get('companyName', '').strip() or "Name Not Available"

    authorised_share = 0
    try:
        authorised_share = int(str(other_sections.get('authorisedShare', '0')).replace(',', ''))
    except (ValueError, TypeError):
        pass
    
    cleaned_shareholder_details = []
    for key, shareholder_data in shareholder_details_raw.items():
        if not shareholder_data:
            continue
        
        cleaned_data = clean_person_details(shareholder_data, postcode_data)
        
        # Add share count and value to each shareholder
        shareholder_id = cleaned_data.get('shareholderId')
        if shareholder_id and shareholder_id in parsed_shares_info:
            share_info = parsed_shares_info[shareholder_id]
            shares_count = share_info.get('shares_count', 0)
            cleaned_data['shares_count'] = shares_count
            cleaned_data['share_value'] = shares_count * value_per_share
        else:
            cleaned_data['shares_count'] = 0
            cleaned_data['share_value'] = 0.0

        if cleaned_data.get('type') == 'Other':
            name_from_key = key.split(',', 1)[-1].strip() if ',' in key else key
            cleaned_data['lastName'] = name_from_key
            cleaned_data['firstName'] = ''
            cleaned_data['middleName'] = ''
            
        cleaned_shareholder_details.append(cleaned_data)
    
    return {
        "id": company_id,
        "company_name": company_name,
        "registration_type": "company",
        "incorporation_number": company_info.get('incorporationNumber'),
        "tin": company_info.get('tin'),
        "incorporation_date": company_info.get('incorporationDate'),
        "accounting_date": company_info.get('accountingDate'),
        "company_type": company_info.get('companyType'),
        "applicant_info": clean_person_details(applicant_info, postcode_data),
        "business_place": data.get('principalPlaceOfBusinessObj') or {},
        "company_secretary_detail": clean_person_details(data.get('companySecretaryDetail') or {}, postcode_data),
        "other_info": other_sections,
        "director_details": [clean_person_details(p, postcode_data) for p in (data.get('directorDetail') or {}).values() if p],
        "shareholder_details": cleaned_shareholder_details,
        "business_activities": data.get('allBusinessActivitiesArray', []),
        "created_at": datetime.now().isoformat(),
        "authorised_share": authorised_share,
        "time_application_created": applicant_info.get('timeApplicationCreated', ''),
        "time_application_submitted": applicant_info.get('timeApplicationSubmitted', ''),
        "service_type": applicant_info.get('serviceType', 'Unknown')
    }

def extract_people_from_company(company_doc: dict) -> Generator[Dict[str, Any], None, None]:
    """Extracts individual person documents from a cleaned company document."""
    record_id = company_doc['id']
    incorporation_number = company_doc.get('incorporation_number')
    reg_id_for_grouping = incorporation_number if incorporation_number and incorporation_number.strip() else record_id
    reg_name = company_doc['company_name']
    approval_status = company_doc.get('applicant_info', {}).get('approvalStatus', 'Unknown')
    record_created_at = company_doc.get('time_application_created')

    person_shareholders = [
        p for p in company_doc.get('shareholder_details', []) if p and p.get('type') == 'Natural person'
    ]

    people_map = {
        'director': company_doc.get('director_details', []),
        'shareholder': person_shareholders,
        'applicant': [company_doc.get('applicant_info')],
        'secretary': [company_doc.get('company_secretary_detail')]
    }

    for role, details_list in people_map.items():
        for details in details_list:
            if not isinstance(details, dict):
                continue
            
            person_identifier = (details.get('nationalId') or details.get('nationalID') or details.get('passportNumber', '')).strip()
            if not person_identifier:
                continue

            full_name = f"{details.get('firstName', '')} {details.get('middleName', '')} {details.get('lastName', '')}".strip()
            
            # **FIX**: Use the unique record_id for the document _id to prevent overwrites and errors.
            yield {
                "_index": "people",
                "_id": f"{person_identifier}_{role}_{record_id}",
                "_source": {
                    "person_identifier": person_identifier,
                    "full_name": full_name,
                    "date_of_birth": format_date_for_es(details.get('dateOfBirth')),
                    "nationality": details.get('nationality') or details.get('nationalityApplicant'),
                    "role": role,
                    "registration_id": reg_id_for_grouping,
                    "source_record_id": record_id,
                    "registration_name": reg_name,
                    "registration_type": "company",
                    "approval_status": approval_status,
                    "record_created_at": record_created_at,
                    "details": details
                }
            }


# --- Business Name Processors ---
def clean_and_prepare_business_name(bn_id: str, data: dict, postcode_data: Dict) -> dict:
    """Cleans and prepares a single business name document for Elasticsearch indexing."""
    business_info = data.get('businessInfoObj') or {}
    applicant_info = data.get('applicantObj') or {}
    
    # Normalize approval status
    status = applicant_info.get('approvalStatus', 'Unknown')
    if status == 'noResult':
        status = 'Unknown'
    applicant_info['approvalStatus'] = status
    
    return {
        "id": bn_id,
        "business_name": business_info.get('businessName', '').strip() or "Name Not Available",
        "registration_type": "business-name",
        "registration_number": business_info.get('registrationNumber'),
        "registration_date": business_info.get('registrationDate'),
        "business_type": business_info.get('businessType'),
        "state": business_info.get('state'),
        "applicant_info": clean_person_details(applicant_info, postcode_data),
        "business_place": data.get('principalPlaceOfBusinessObj') or {},
        "owner_details": [clean_person_details(p, postcode_data) for p in (data.get('ownerDetail') or {}).values() if p],
        "persons_who_can_update_details": [clean_person_details(p, postcode_data) for p in (data.get('personsWhoDetail') or {}).values() if p],
        "bank_operator_details": [clean_person_details(p, postcode_data) for p in (data.get('otherBankDetail') or {}).values() if p],
        "business_activities": data.get('allBusinessActivitiesArray', []),
        "created_at": datetime.now().isoformat(),
        "time_application_created": applicant_info.get('timeApplicationCreated', '')
    }

def extract_people_from_business_name(bn_doc: dict) -> Generator[Dict[str, Any], None, None]:
    """Extracts individual person documents from a cleaned business name document."""
    reg_id = bn_doc['id']
    reg_name = bn_doc['business_name']
    approval_status = bn_doc.get('applicant_info', {}).get('approvalStatus', 'Unknown')
    record_created_at = bn_doc.get('time_application_created')
    
    people_map = {
        'owner': bn_doc.get('owner_details', []),
        'authorized_person': bn_doc.get('persons_who_can_update_details', []),
        'bank_operator': bn_doc.get('bank_operator_details', []),
        'applicant': [bn_doc.get('applicant_info')]
    }
    
    for role, details_list in people_map.items():
        for details in details_list:
            if not isinstance(details, dict):
                continue
            
            person_identifier = (details.get('nationalId') or details.get('nationalID') or details.get('passportNumber', '')).strip()
            if not person_identifier:
                continue
            
            full_name = f"{details.get('firstName', '')} {details.get('middleName', '')} {details.get('lastName', '')}".strip()

            # **FIX**: Use the unique record_id for the document _id. Here, reg_id is the unique ID.
            yield {
                "_index": "people",
                "_id": f"{person_identifier}_{role}_{reg_id}",
                "_source": {
                    "person_identifier": person_identifier,
                    "full_name": full_name,
                    "date_of_birth": format_date_for_es(details.get('dateOfBirth')),
                    "nationality": details.get('nationality') or details.get('nationalityApplicant'),
                    "role": role,
                    "registration_id": reg_id,
                    "source_record_id": reg_id, # For business names, they are the same
                    "registration_name": reg_name,
                    "registration_type": "business-name",
                    "approval_status": approval_status,
                    "record_created_at": record_created_at,
                    "details": details
                }
            }
```

---
**6. Modified File: `backend_python/migration/index_from_json.py`**

```python
# backend_python/migration/index_from_json.py
import sys
from pathlib import Path
import json
import logging
import time
import itertools
from datetime import datetime
from elasticsearch.helpers import bulk
from tqdm import tqdm

# Add project root to path to allow importing app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.elasticsearch_client import es_client
from .utils import (
    format_duration,
    count_docs_in_json_dir,
    generate_es_actions,
    count_people_in_json_dir,
    generate_people_actions,
    count_docs_in_json_list_dir,
    generate_es_actions_from_list,
    count_rows_in_csv,
    generate_es_actions_from_csv
)
from .processors import (
    aggregate_corporate_shareholders,
    clean_and_prepare_company,
    extract_people_from_company,
    clean_and_prepare_business_name,
    extract_people_from_business_name,
    clean_and_prepare_tra_tin,
    clean_and_prepare_wabunge
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("=== Starting Data Indexing to Elasticsearch (Refactored) ===")
    overall_start_time = time.time()

    json_base_dir = "/app/main_jsons"
    data_base_dir = "/app/data"
    postcode_file = f"{data_base_dir}/post_Codes_all_Tz.json"
    wabunge_file = f"{data_base_dir}/wabunge.csv"
    company_json_dir = f"{json_base_dir}/companies"
    bn_json_dir = f"{json_base_dir}/business_names"
    tra_tins_json_dir = f"{json_base_dir}/Tra_TINS"

    all_stats = {}

    try:
        es_client.connect()

        logger.info("Creating fresh indices...")
        es_client.create_indices()

        with open(postcode_file, 'r', encoding='utf-8') as f:
            postcode_data = json.load(f)
        logger.info(f"Loaded {len(postcode_data)} postcode records.")
        
        # --- Index Wabunge from CSV ---
        total_wabunge = count_rows_in_csv(wabunge_file)
        if total_wabunge > 0:
            logger.info(f"Starting Wabunge indexing for {total_wabunge:,} rows...")
            wabunge_actions_gen = generate_es_actions_from_csv(wabunge_file, "wabunge", clean_and_prepare_wabunge)
            progress = tqdm(wabunge_actions_gen, total=total_wabunge, desc="Indexing Wabunge", unit="rows")
            success, errors = bulk(es_client.client, progress, raise_on_error=False, chunk_size=1000)
            all_stats['wabunge'] = {'processed': total_wabunge, 'indexed': success, 'errors': len(errors)}
            if errors:
                logger.warning(f"Encountered {len(errors)} errors while indexing Wabunge. First error: {errors[0]}")

        # --- Index TRA TINS ---
        total_tins = count_docs_in_json_list_dir(tra_tins_json_dir)
        if total_tins > 0:
            logger.info(f"Starting TRA TINs indexing for {total_tins:,} documents...")
            tins_actions_gen = generate_es_actions_from_list(tra_tins_json_dir, "tra_tins", clean_and_prepare_tra_tin, postcode_data)
            progress = tqdm(tins_actions_gen, total=total_tins, desc="Indexing TRA TINs", unit="docs")
            success, errors = bulk(es_client.client, progress, raise_on_error=False, chunk_size=1000)
            all_stats['tra_tins'] = {'processed': total_tins, 'indexed': success, 'errors': len(errors)}
            if errors:
                logger.warning(f"Encountered {len(errors)} errors while indexing TRA Tins. First error: {errors[0]}")

        # --- Index Corporate Shareholders ---
        corporate_shareholders_agg = aggregate_corporate_shareholders(company_json_dir)
        total_corp_shareholders = len(corporate_shareholders_agg)
        logger.info(f"Found {total_corp_shareholders:,} unique corporate shareholders.")
        if total_corp_shareholders > 0:
            actions = []
            for sh_id, data in corporate_shareholders_agg.items():
                data['shareholding_count'] = len(data['shareholdings'])
                data['created_at'] = datetime.now().isoformat()
                actions.append({"_index": "corporate_shareholders", "_id": sh_id, "_source": data})
            
            progress = tqdm(actions, total=total_corp_shareholders, desc="Indexing Corporate Shareholders", unit="docs")
            success, errors = bulk(es_client.client, progress, raise_on_error=False, chunk_size=1000)
            all_stats['corporate_shareholders'] = {'processed': total_corp_shareholders, 'indexed': success, 'errors': len(errors)}
            if errors:
                logger.warning(f"Encountered {len(errors)} errors while indexing corporate shareholders. First error: {errors[0]}")
        
        # --- Index Companies ---
        total_companies = count_docs_in_json_dir(company_json_dir)
        if total_companies > 0:
            logger.info(f"Starting company indexing for {total_companies:,} documents...")
            company_actions_gen = generate_es_actions(company_json_dir, "companies", clean_and_prepare_company, postcode_data)
            progress = tqdm(company_actions_gen, total=total_companies, desc="Indexing Companies", unit="docs")
            success, errors = bulk(es_client.client, progress, raise_on_error=False, chunk_size=1000)
            all_stats['companies'] = {'processed': total_companies, 'indexed': success, 'errors': len(errors)}
            if errors:
                logger.warning(f"Encountered {len(errors)} errors while indexing companies. First error: {errors[0]}")

        # --- Index Business Names ---
        total_bns = count_docs_in_json_dir(bn_json_dir)
        if total_bns > 0:
            logger.info(f"Starting business name indexing for {total_bns:,} documents...")
            bn_actions_gen = generate_es_actions(bn_json_dir, "business_names", clean_and_prepare_business_name, postcode_data)
            progress = tqdm(bn_actions_gen, total=total_bns, desc="Indexing Business Names", unit="docs")
            success, errors = bulk(es_client.client, progress, raise_on_error=False, chunk_size=1000)
            all_stats['business_names'] = {'processed': total_bns, 'indexed': success, 'errors': len(errors)}
            if errors:
                logger.warning(f"Encountered {len(errors)} errors while indexing business names. First error: {errors[0]}")

        # --- Index People ---
        logger.info("Pre-calculating total number of people records...")
        total_people_from_companies = count_people_in_json_dir(company_json_dir, clean_and_prepare_company, extract_people_from_company, postcode_data)
        total_people_from_bns = count_people_in_json_dir(bn_json_dir, clean_and_prepare_business_name, extract_people_from_business_name, postcode_data)
        total_people = total_people_from_companies + total_people_from_bns
        logger.info(f"Found a total of {total_people:,} people records to index.")
        
        if total_people > 0:
            company_people_gen = generate_people_actions(company_json_dir, clean_and_prepare_company, extract_people_from_company, postcode_data)
            bn_people_gen = generate_people_actions(bn_json_dir, clean_and_prepare_business_name, extract_people_from_business_name, postcode_data)
            
            all_people_actions = itertools.chain(company_people_gen, bn_people_gen)
            progress = tqdm(all_people_actions, total=total_people, desc="Indexing People", unit="person")
            
            success, errors = bulk(es_client.client, progress, raise_on_error=False, chunk_size=2000)
            all_stats['people'] = {'processed': total_people, 'indexed': success, 'errors': len(errors)}
            if errors:
                logger.warning(f"Encountered {len(errors)} errors while indexing people. First error: {errors[0]}")
        else:
            all_stats['people'] = {'processed': 0, 'indexed': 0, 'errors': 0}

    except Exception as e:
        logger.error(f"Migration script failed with a critical error: {str(e)}", exc_info=True)
    finally:
        if es_client.client:
            es_client.close()
        
        total_duration = time.time() - overall_start_time
        # --- Final Summary ---
        summary = f"""
================================================================================
INDEXING SUMMARY
================================================================================
Total time taken: {format_duration(total_duration)}

Wabunge (from CSV):
- Documents to process: {all_stats.get('wabunge', {}).get('processed', 0):,}
- Successfully indexed: {all_stats.get('wabunge', {}).get('indexed', 0):,}
- Errors: {all_stats.get('wabunge', {}).get('errors', 0):,}

TRA TINS:
- Documents to process: {all_stats.get('tra_tins', {}).get('processed', 0):,}
- Successfully indexed: {all_stats.get('tra_tins', {}).get('indexed', 0):,}
- Errors: {all_stats.get('tra_tins', {}).get('errors', 0):,}

Corporate Shareholders:
- Documents to process: {all_stats.get('corporate_shareholders', {}).get('processed', 0):,}
- Successfully indexed: {all_stats.get('corporate_shareholders', {}).get('indexed', 0):,}
- Errors: {all_stats.get('corporate_shareholders', {}).get('errors', 0):,}

Companies:
- Documents to process: {all_stats.get('companies', {}).get('processed', 0):,}
- Successfully indexed: {all_stats.get('companies', {}).get('indexed', 0):,}
- Errors: {all_stats.get('companies', {}).get('errors', 0):,}

Business Names:
- Documents to process: {all_stats.get('business_names', {}).get('processed', 0):,}
- Successfully indexed: {all_stats.get('business_names', {}).get('indexed', 0):,}
- Errors: {all_stats.get('business_names', {}).get('errors', 0):,}

People (from all sources):
- Records to process: {all_stats.get('people', {}).get('processed', 0):,}
- Successfully indexed: {all_stats.get('people', {}).get('indexed', 0):,}
- Errors: {all_stats.get('people', {}).get('errors', 0):,}
================================================================================
"""
        print(summary)


if __name__ == "__main__":
    main()
```

---
**7. Modified File: `backend_python/app/routes/__init__.py`**

```python
# backend_python/app/routes/__init__.py
from .companies import router as companies_router
from .business_names import router as business_names_router
from .people import router as people_router
from .person import router as person_router
from .business_activities import router as business_activities_router
from .search import router as search_router
from .network import router as network_router
from .corporate_shareholders import router as corporate_shareholders_router
from .influencers import router as influencers_router
from .phone_filter import phone_filter_router
from .tra_tins import router as tra_tins_router
from .wabunge import router as wabunge_router

__all__ = [
    "companies_router",
    "business_names_router",
    "people_router",
    "person_router",
    "business_activities_router",
    "search_router",
    "network_router",
    "corporate_shareholders_router",
    "influencers_router",
    "phone_filter_router",
    "tra_tins_router",
    "wabunge_router"
]
```

---
**8. Modified File: `backend_python/app/main.py`**

```python
# backend_python/app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from contextlib import asynccontextmanager

from .config import settings
from .elasticsearch_client import es_client
from .routes import (
    companies_router,
    business_names_router,
    people_router,
    person_router,
    business_activities_router,
    search_router,
    network_router,
    corporate_shareholders_router,
    influencers_router,
    phone_filter_router,
    tra_tins_router,
    wabunge_router
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    logger.info("Starting Leads WebApp Backend...")
    logger.info(f"Elasticsearch URL: {settings.elasticsearch_url}")
    logger.info(f"CORS Origins: {settings.cors_origins_list}")
    try:
        es_client.connect()
        es_client.connect_async()
        logger.info("Application startup complete")
    except Exception as e:
        logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
        logger.warning("Application will start but may not function properly")
    
    yield
    
    logger.info("Shutting down Leads WebApp Backend...")
    try:
        es_client.close()
        await es_client.close_async()
        logger.info("Application shutdown complete")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")

app = FastAPI(
    title="Leads WebApp API",
    description="High-performance API for corporate data exploration using FastAPI and Elasticsearch",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(companies_router)
app.include_router(business_names_router)
app.include_router(corporate_shareholders_router)
app.include_router(people_router)
app.include_router(person_router)
app.include_router(business_activities_router)
app.include_router(search_router)
app.include_router(network_router)
app.include_router(influencers_router)
app.include_router(phone_filter_router)
app.include_router(tra_tins_router)
app.include_router(wabunge_router)

@app.get("/")
async def root():
    return {
        "message": "Leads WebApp API v2.0",
        "status": "running",
        "backend": "Python FastAPI + Elasticsearch"
    }

@app.get("/health")
async def health_check():
    try:
        if es_client.async_client is None:
            return {"status": "unhealthy", "error": "Elasticsearch client not initialized"}
        
        es_health = await es_client.async_client.cluster.health()
        return {
            "status": "healthy",
            "elasticsearch": {
                "status": es_health["status"],
                "cluster_name": es_health["cluster_name"],
                "number_of_nodes": es_health["number_of_nodes"]
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {"status": "unhealthy", "error": str(e)}
```

### Frontend File Modifications (`leads_webapp1`)

---
**9. Modified File: `leads_webapp1/src/types.ts`**

```typescript
// leads_webapp1/src/types.ts

// A generic type for different registration types
export type RegistrationType = 'company' | 'business-name';

// --- Shared Person/Contact Interfaces ---
export interface PersonDetail {
  dateOfBirth: string;
  firstName: string;
  middleName: string;
  lastName: string;
  nationalId: string;
  emailAddress: string;
  mobilePhoneNumber: string;
  passportNumber: string;
  tin?: string;
  type?: string; // "Natural person" or "Other"
  originOf: string;
  nationality: string;
  verification?: string;
  country: string;
  genderFemale: boolean;
  genderMale: boolean;
  typeOfLocalAddress?: string;
  region: string;
  district: string;
  ward: string;
  postcode: string;
  addressUnSurveyed?: string;
  address?: string;
  street: string;
  road: string;
  plotNumber: string;
  blockNumber: string;
  houseNumber: string;
  shareholderId?: string;
}

export interface Applicant {
  trackingNumber: string;
  timeApplicationCreated: string;
  timeApplicationSubmitted: string;
  approvalStatus: string;
  serviceType: string;
  dateOfBirth: string;
  firstName: string;
  middleName: string;
  lastName: string;
  emailAddress: string;
  passportNumber: string;
  nationalityApplicant: string;
  mobilePhoneNumber: string;
  nationalID: string;
  genderMale: boolean;
  genderFemale: boolean;
}

// --- Company Specific Interfaces ---
export interface CompanyInfo {
  companyType: string;
  companyName: string;
  incorporationDate: string;
  accountingDate: string;
  incorporationNumber: string;
  tin: string;
  authorisedShare?: string;
}

export interface CompanyBusinessPlace {
  country: string;
  typeOfLocalAddress: string;
  region: string;
  district: string;
  ward: string;
  postcode: string;
  poBox: string;
  emailAddressCmpny: string;
  mobilePhoneNumberCmpny: string;
  streetBusiness: string;
  roadBusiness: string;
  plotNumberBusiness: string;
  blockNumberBusiness: string;
  houseNumberBusiness: string;
}

export interface CompanySecretary {
  // ... same structure as PersonDetail
  [key: string]: any;
}

export interface OtherSections {
  authorisedShare: string;
  numberOfShares: string;
  sharesTaken: string;
  numberOfRecordsPagination: string;
  paymentOrderDate: string;
}

export interface TimelineEntry {
  recordId: string;
  serviceType: string;
  timeApplicationCreated: string;
  timeApplicationSubmitted: string;
}

export interface Company {
  id: string; // This is the RECORD id
  registrationType: 'company';
  applicantObj: Applicant;
  companyInfoObj: CompanyInfo;
  principalPlaceOfBusinessObj: CompanyBusinessPlace;
  allBusinessActivitiesArray: string[];
  directorDetail: PersonDetail[];
  shareholderDetail: PersonDetail[]; // Can contain persons or corporate shells
  companySecretaryDetail: CompanySecretary;
  otherRemainingSections: OtherSections;
  timeline: TimelineEntry[];
  highlight?: { [key: string]: string[] };
}

// --- Business Name Specific Interfaces ---
export interface BusinessInfo {
  businessType: string;
  businessName: string;
  registrationDate: string;
  registrationNumber: string;
  state: string;
}

export interface BusinessPlace {
  typeOfLocalAddress: string;
  unsurveyedAddress: string;
  region: string;
  district: string;
  ward: string;
  postcode: string;
  poBox: string;
  emailAddressBsness: string;
  mobilePhoneNumberBsness: string;
  streetBusiness: string;
  roadBusiness: string;
  plotNumberBusiness: string;
  blockNumberBusiness: string;
  houseNumberBusiness: string;
}

export interface BusinessName {
  id: string;
  registrationType: 'business-name';
  applicantObj: Applicant;
  businessInfoObj: BusinessInfo;
  principalPlaceOfBusinessObj: BusinessPlace;
  allBusinessActivitiesArray: string[];
  ownerDetail: PersonDetail[];
  personsWhoDetail: PersonDetail[];
  otherBankDetail: PersonDetail[];
  highlight?: { [key: string]: string[] };
}

// --- NEW Corporate Shareholder Types ---
export interface CorporateShareholderListItem {
  id: string;
  name: string;
  country: string;
  shareholdingCount: number;
  highlight?: { [key: string]: string[] };
}

export interface Shareholding {
  companyId: string; // This is the company's RECORD ID for linking to the detail page
  companyName: string;
  sharesClass: string;
  sharesCount: number;
  approvalStatus?: string;
}

export interface CorporateShareholderDetail {
  id: string;
  name: string;
  country: string;
  emailAddress: string;
  phoneNumber: string;
  shareholdings: Shareholding[];
}


// --- Union and Generic Types ---
export type Registration = Company | BusinessName;

// Generic list item for dashboards
export interface RegistrationListItem {
  id: string; // This is the RECORD id
  registrationType: RegistrationType;
  applicantObj: {
    approvalStatus: string;
    timeApplicationCreated: string;
  };
  companyInfoObj?: CompanyInfo;
  businessInfoObj?: BusinessInfo;
  highlight?: { [key: string]: string[] };
}

// --- People and Contact Search Types ---
export interface Person {
  id: string;
  fullName: string;
  dateOfBirth: string | null;
  nationality: string;
  role: string;
  registrationId: string; // This is incorporationNumber for companies (for grouping)
  sourceRecordId: string; // The specific record ID for linking
  registrationName: string;
  registrationType: RegistrationType;
  highlight?: { [key: string]: string[] };
}

export interface PersonRole {
  role: string; // This can now be comma-separated roles
  registrationId: string; // The stable identifier (e.g. incorporationNumber)
  sourceRecordId: string; // The specific record ID for linking
  registrationName: string;
  registrationType: RegistrationType;
  approvalStatus?: string;
}

export interface PersonDetails {
  fullName: string;
  dateOfBirth: string | null;
  nationality: string;
}

export type PersonProfileDetails = Partial<
  Omit<PersonDetail, 'emailAddress' | 'mobilePhoneNumber'> &
  Omit<Applicant, 'emailAddress' | 'mobilePhoneNumber'> &
  {
    nida_region?: string;
    nida_district?: string;
    nida_ward?: string;
    emailAddress: string[];
    mobilePhoneNumber: string[];
  }
>;

export interface ShareholdingInfo {
  companyId: string; // This is the specific record ID
  companyName: string;
  shareValue: number;
}

export interface AssociatedTin {
  id: string;
  tinNo: string;
  name: string | null;
  tinType: string | null;
  postalCity: string | null;
  businessStartDate: string | null;
  telephones: string[];
}

export interface FullPersonProfile {
  details: PersonDetails;
  roles: PersonRole[];
  detailedInfo: PersonProfileDetails | null;
  shareholdings?: ShareholdingInfo[];
  associatedTins?: AssociatedTin[];
}

export interface CompanyContactResult {
  companyId: string; // This is the record ID
  companyName: string;
  foundInSections: string;
  registrationType: 'company';
  approvalStatus?: string;
}

export interface BusinessNameContactResult {
  businessNameId: string;
  businessName: string;
  foundInSections: string;
  registrationType: 'business-name';
  approvalStatus?: string;
}

export interface PersonContactResult {
  personId: string;
  fullName: string;
  role: string;
  registrationId: string;
  registrationName: string;
  registrationType: RegistrationType;
  approvalStatus?: string;
}

export interface ContactSearchData {
  companies: CompanyContactResult[];
  business_names: BusinessNameContactResult[];
  people: PersonContactResult[];
  associatedTins: AssociatedTin[];
}

// --- Network Graph Types ---
export interface GraphNode {
  id: string;
  name: string;
  type: 'person' | 'company' | 'business-name' | 'corporate-shareholder';
  val?: number;
  mostCommonRole?: string;
  dateOfBirth?: string;
  gender?: 'male' | 'female';
}

export interface GraphLink {
  source: string;
  target: string;
  roles: string[];
}

export interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
}


// --- Key Influencers Types ---
export interface LeaderboardPerson {
  id: string;
  name: string;
  value: number;
  nationality: string | null;
}

export interface CoDirectorMatrixEntry {
  person1_id: string;
  person1_name: string;
  person2_id: string;
  person2_name: string;
  count: number;
}

export interface CoDirectorMatrixData {
  matrix: CoDirectorMatrixEntry[];
  people: { id: string, name: string }[];
}


export interface PathNode {
  id: string;
  name: string;
  type: string;
}

export interface PathLink {
  source: string;
  target: string;
  label: string;
}

export interface SixDegreesPath {
  path_found: boolean;
  message: string;
  nodes?: PathNode[];
  links?: PathLink[];
}

export interface PhoneFilterPerson {
  id: string;
  fullName: string;
  age: number | null;
  mobilePhoneNumber: string | null;
  nationality: string | null;
  formattedNumber: string | null;
  standardNumber: string | null;
  highlight?: { [key: string]: string[] };
}

// --- NEW: Contact Exporter Types ---
export interface ContactExportPreviewStats {
  initial_count: number;
  duplicates_removed: number;
  engaged_removed: number;
  final_count: number;
  sample_size: number;
}
export interface ContactExportPreviewData {
  stats: ContactExportPreviewStats;
  sample_data_head: { Name: string; 'Phone Number': string }[];
  sample_data_tail: { Name: string; 'Phone Number': string }[];
}


// --- TRA TINs Types ---
export interface TraTinListItem {
  id: string;
  tinNo: string;
  name: string | null;
  tinType: string | null;
  postalCity: string | null;
  businessStartDate: string | null;
  telephones: string[];
  age: number | null;
  highlight?: { [key: string]: string[] };
}

export interface TraTinDetail extends TraTinListItem {
  firstname?: string;
  middlename?: string;
  lastname?: string;
  national_id?: string;
  pobox?: string;
  postal_city?: string;
  date_of_birth?: string;
  nida_region?: string;
  nida_district?: string;
  nida_ward?: string;
}

// --- Wabunge Types ---
export interface WabungeListItem {
  id: string;
  name: string;
  title: string | null;
  phone1: string | null;
  phone2: string | null;
  chama: string | null;
  address1: string | null;
  address2: string | null;
  highlight?: { [key: string]: string[] };
}
```

---
**10. Modified File: `leads_webapp1/src/components/Header.tsx`**

```typescript
// leads_webapp1/src/components/Header.tsx
import React from 'react';
import { Link, NavLink } from 'react-router-dom';
import { ApprovedOnlyToggle } from './ApprovedOnlyToggle';

interface HeaderProps {
  onToggleSidebar: () => void;
}

export const Header: React.FC<HeaderProps> = ({ onToggleSidebar }) => {
  const companyLinks = [
    { path: '/company/dashboard', label: 'Dashboard' },
    { path: '/company/people', label: 'People' },
    { path: '/company/activities', label: 'Activities' },
  ];
  const businessNameLinks = [
    { path: '/business-name/dashboard', label: 'Dashboard' },
    { path: '/business-name/people', label: 'People' },
    { path: '/business-name/activities', label: 'Activities' },
  ];
  const contactLinks = [
    { path: '/contacts', label: 'Contact Directory' },
    { path: '/phone-filter', label: 'Phone Filter' },
  ];
  const otherEntityLinks = [
    { path: '/corporate-shareholders', label: 'Corporate Shareholders' },
    { path: '/tra-tins', label: 'TRA Tins' },
    { path: '/wabunge', label: 'Wabunge' },
  ];
  const vizLinks = [
    { path: '/network-graph', label: 'Network Graph' },
    { path: '/key-influencers', label: 'Key Influencers' },
  ];

  return (
    <header className="bg-white dark:bg-slate-900 shadow-sm sticky top-0 z-10">
      <div className="container mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          <div className="flex items-center space-x-4">
            <button
              onClick={onToggleSidebar}
              className="text-slate-600 dark:text-slate-400"
            >
              <span className="material-icons">menu</span>
            </button>
            <Link to="/" className="flex items-center gap-2">
              <span className="material-icons text-primary text-3xl">business_center</span>
              <span className="text-xl font-bold text-slate-900 dark:text-white">Corporate Data Explorer</span>
            </Link>
          </div>
          <nav className="hidden lg:flex items-center space-x-1">

            {/* Companies Menu */}
            <div className="relative group">
              <button className="flex items-center text-sm font-medium text-slate-600 hover:text-primary dark:text-slate-300 dark:hover:text-primary transition-colors px-3 py-2">
                Companies
                <span className="material-icons text-base">expand_more</span>
              </button>
              <div className="absolute top-full left-1/2 -translate-x-1/2 mt-2 p-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg shadow-xl z-50 min-w-[200px] hidden group-hover:block">
                {companyLinks.map(link => (
                  <NavLink
                    key={link.path}
                    to={link.path}
                    className={({isActive}) => `block px-4 py-2 rounded-md text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 ${isActive && 'bg-slate-100 dark:bg-slate-800 font-semibold'}`}
                  >
                    {link.label}
                  </NavLink>
                ))}
              </div>
            </div>

            {/* Business Names Menu */}
            <div className="relative group">
              <button className="flex items-center text-sm font-medium text-slate-600 hover:text-primary dark:text-slate-300 dark:hover:text-primary transition-colors px-3 py-2">
                Business Names
                <span className="material-icons text-base">expand_more</span>
              </button>
              <div className="absolute top-full left-1/2 -translate-x-1/2 mt-2 p-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg shadow-xl z-50 min-w-[200px] hidden group-hover:block">
                {businessNameLinks.map(link => (
                  <NavLink
                    key={link.path}
                    to={link.path}
                    className={({isActive}) => `block px-4 py-2 rounded-md text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 ${isActive && 'bg-slate-100 dark:bg-slate-800 font-semibold'}`}
                  >
                    {link.label}
                  </NavLink>
                ))}
              </div>
            </div>

            {/* Other Entities Menu */}
            <div className="relative group">
              <button className="flex items-center text-sm font-medium text-slate-600 hover:text-primary dark:text-slate-300 dark:hover:text-primary transition-colors px-3 py-2">
                Other Entities
                <span className="material-icons text-base">expand_more</span>
              </button>
              <div className="absolute top-full left-1/2 -translate-x-1/2 mt-2 p-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg shadow-xl z-50 min-w-[200px] hidden group-hover:block">
                {otherEntityLinks.map(link => (
                  <NavLink
                    key={link.path}
                    to={link.path}
                    className={({isActive}) => `block px-4 py-2 rounded-md text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 ${isActive && 'bg-slate-100 dark:bg-slate-800 font-semibold'}`}
                  >
                    {link.label}
                  </NavLink>
                ))}
              </div>
            </div>
            
            {/* Contacts Menu */}
            <div className="relative group">
              <button className="flex items-center text-sm font-medium text-slate-600 hover:text-primary dark:text-slate-300 dark:hover:text-primary transition-colors px-3 py-2">
                Contacts
                <span className="material-icons text-base">expand_more</span>
              </button>
              <div className="absolute top-full left-1/2 -translate-x-1/2 mt-2 p-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg shadow-xl z-50 min-w-[200px] hidden group-hover:block">
                {contactLinks.map(link => (
                  <NavLink
                    key={link.path}
                    to={link.path}
                    className={({isActive}) => `block px-4 py-2 rounded-md text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 ${isActive && 'bg-slate-100 dark:bg-slate-800 font-semibold'}`}
                  >
                    {link.label}
                  </NavLink>
                ))}
              </div>
            </div>

            {/* Visualizations Menu */}
            <div className="relative group">
              <button className="flex items-center text-sm font-medium text-slate-600 hover:text-primary dark:text-slate-300 dark:hover:text-primary transition-colors px-3 py-2">
                Visualizations
                <span className="material-icons text-base">expand_more</span>
              </button>
              <div className="absolute top-full right-0 mt-2 p-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg shadow-xl z-50 min-w-[200px] hidden group-hover:block">
                {vizLinks.map(link => (
                  <NavLink
                    key={link.path}
                    to={link.path}
                    className={({isActive}) => `block px-4 py-2 rounded-md text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 ${isActive && 'bg-slate-100 dark:bg-slate-800 font-semibold'}`}
                  >
                    {link.label}
                  </NavLink>
                ))}
              </div>
            </div>

            {/* Approved Only Toggle */}
            <ApprovedOnlyToggle className="pl-4" />
          </nav>
        </div>
      </div>
    </header>
  );
};
```

---
**11. Modified File: `leads_webapp1/src/components/Sidebar.tsx`**

```typescript
// leads_webapp1/src/components/Sidebar.tsx
import React from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import { ApprovedOnlyToggle } from './ApprovedOnlyToggle';

const HomeIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path d="M10.707 2.293a1 1 0 00-1.414 0l-7 7a1 1 0 001.414 1.414L4 10.414V17a1 1 0 001 1h2a1 1 0 001-1v-2a1 1 0 011-1h2a1 1 0 011 1v2a1 1 0 001 1h2a1 1 0 001-1v-6.586l.293.293a1 1 0 001.414-1.414l-7-7z" /></svg>;
const UsersIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path d="M9 6a3 3 0 11-6 0 3 3 0 016 0zM17 6a3 3 0 11-6 0 3 3 0 016 0zM12.93 17c.046-.327.07-.66.07-1a6.97 6.97 0 00-1.5-4.33A5 5 0 0119 16v1h-6.07zM6 11a5 5 0 015 5v1H1v-1a5 5 0 015-5z" /></svg>;
const BriefcaseIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M6 6V5a3 3 0 013-3h2a3 3 0 013 3v1h2a2 2 0 012 2v3.57A22.952 22.952 0 0110 13a22.95 22.95 0 01-8-1.43V8a2 2 0 012-2h2zm2-1a1 1 0 011-1h2a1 1 0 011 1v1H8V5zm1 5a1 1 0 011-1h.01a1 1 0 110 2H10a1 1 0 01-1-1z" clipRule="evenodd" /><path d="M2 13.692V16a2 2 0 002 2h12a2 2 0 002-2v-2.308A24.974 24.974 0 0110 15c-2.796 0-5.487-.46-8-1.308z" /></svg>;
const ContactsIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M10 2a1 1 0 00-1 1v1a1 1 0 002 0V3a1 1 0 00-1-1zM4 4a1 1 0 00-1 1v12a1 1 0 001 1h12a1 1 0 001-1V5a1 1 0 00-1-1H4zM3 15a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zM8 6a1 1 0 000 2h4a1 1 0 100-2H8z" clipRule="evenodd" /></svg>;
const NetworkIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path d="M15 8a3 3 0 10-2.977-2.63l-4.94 2.47a3 3 0 100 4.319l4.94 2.47a3 3 0 10.895-1.789l-4.94-2.47a3.027 3.027 0 000-.74l4.94-2.47C13.456 7.68 14.19 8 15 8z" /></svg>;
const CorporateIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M4 4a2 2 0 00-2 2v8a2 2 0 002 2h12a2 2 0 002-2V8a2 2 0 00-2-2h-5L9 4H4zm2 6a1 1 0 011-1h6a1 1 0 110 2H7a1 1 0 01-1-1z" clipRule="evenodd" /></svg>;
const InfluencerIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M10 9a3 3 0 100-6 3 3 0 000 6zm-7 9a7 7 0 1114 0H3z" clipRule="evenodd" /></svg>;
const ReceiptIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M5 4a3 3 0 00-3 3v6a3 3 0 003 3h10a3 3 0 003-3V7a3 3 0 00-3-3H5zm-1 9v-1h5v2H5a1 1 0 01-1-1zm7 1h4a1 1 0 001-1v-1h-5v2zm0-4h5V8h-5v2zM4 8h5v2H4V8zm0 5a1 1 0 011-1h5v2H4v-1z" clipRule="evenodd" /></svg>;
const GroupsIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor"><path d="M13 6a3 3 0 11-6 0 3 3 0 016 0zM18 8a2 2 0 11-4 0 2 2 0 014 0zM14 15a4 4 0 00-8 0v3h8v-3zM6 8a2 2 0 11-4 0 2 2 0 014 0zM16 18v-3a5.972 5.972 0 00-.75-2.906A3.005 3.005 0 0119 15v3h-3zM4.75 12.094A5.973 5.973 0 004 15v3H1v-3a3 3 0 013.75-2.906z" /></svg>;
const CloseIcon: React.FC = () => <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>;


interface SidebarProps {
  isSidebarOpen: boolean;
  toggleSidebar: () => void;
}

const NavItem: React.FC<{ to: string; icon: React.ReactNode; label: string; onClick: () => void; end?: boolean; }> = ({ to, icon, label, onClick, end }) => {
  const activeClassName = "bg-primary text-white";
  const inactiveClassName = "hover:bg-slate-700 text-slate-300";

  return (
    <NavLink
      to={to}
      end={end}
      className={({ isActive }) =>
        `flex items-center gap-3 p-3 rounded-md font-semibold transition-colors duration-200 ${
          isActive ? activeClassName : inactiveClassName
        }`
      }
      onClick={onClick}
    >
      {icon}
      <span className="whitespace-nowrap">{label}</span>
    </NavLink>
  );
};


export const Sidebar: React.FC<SidebarProps> = ({ isSidebarOpen, toggleSidebar }) => {
  const location = useLocation();
  return (
    <>
      <div
        className={`fixed inset-0 bg-black bg-opacity-60 z-30 transition-opacity duration-300 ease-in-out ${
          isSidebarOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'
        }`}
        onClick={toggleSidebar}
        aria-hidden="true"
      />
      <aside
        className={`fixed top-0 left-0 h-full w-64 bg-slate-800 border-r border-slate-700 transform transition-transform duration-300 ease-in-out z-40 ${
          isSidebarOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="flex flex-col h-full">
          <div className="flex items-center justify-between p-4 border-b border-slate-700">
            <h2 className="text-lg font-bold text-white">Menu</h2>
            <button
              onClick={toggleSidebar}
              className="text-slate-400 hover:text-white"
            >
              <CloseIcon />
            </button>
          </div>
          <nav className="flex-grow flex flex-col gap-2 p-2 overflow-y-auto">
              <h3 className="px-3 pt-4 pb-2 text-xs font-bold text-slate-500 uppercase">Companies</h3>
              <NavItem to={'/company/dashboard'} icon={<HomeIcon />} label="Dashboard" onClick={toggleSidebar} end />
              <NavItem to={'/company/people'} icon={<UsersIcon />} label="People" onClick={toggleSidebar} />
              <NavItem to={'/company/activities'} icon={<BriefcaseIcon />} label="Activities" onClick={toggleSidebar} />
              
              <h3 className="px-3 pt-4 pb-2 text-xs font-bold text-slate-500 uppercase">Business Names</h3>
              <NavItem to={'/business-name/dashboard'} icon={<HomeIcon />} label="Dashboard" onClick={toggleSidebar} end />
              <NavItem to={'/business-name/people'} icon={<UsersIcon />} label="People" onClick={toggleSidebar} />
              <NavItem to={'/business-name/activities'} icon={<BriefcaseIcon />} label="Activities" onClick={toggleSidebar} />

              <h3 className="px-3 pt-4 pb-2 text-xs font-bold text-slate-500 uppercase">Other Entities</h3>
              <NavItem to="/corporate-shareholders" icon={<CorporateIcon />} label="Corporate Shareholders" onClick={toggleSidebar} />
              <NavItem to="/tra-tins" icon={<ReceiptIcon />} label="TRA Tins" onClick={toggleSidebar} />
              <NavItem to="/wabunge" icon={<GroupsIcon />} label="Wabunge" onClick={toggleSidebar} />

              <h3 className="px-3 pt-4 pb-2 text-xs font-bold text-slate-500 uppercase">Shared</h3>
              <NavItem to="/contacts" icon={<ContactsIcon />} label="Contact Directory" onClick={toggleSidebar} />
              <NavItem to="/phone-filter" icon={<ContactsIcon />} label="Phone Filter" onClick={toggleSidebar} />

              <h3 className="px-3 pt-4 pb-2 text-xs font-bold text-slate-500 uppercase">Visualizations</h3>
              <NavItem to="/network-graph" icon={<NetworkIcon />} label="Network Graph" onClick={toggleSidebar} />
              <NavItem to="/key-influencers" icon={<InfluencerIcon />} label="Key Influencers" onClick={toggleSidebar} />
            
            {/* Global Filter Section */}
            <div className="mt-auto p-2">
              <ApprovedOnlyToggle className="bg-slate-700/50 p-3 rounded-lg" />
            </div>
          </nav>
        </div>
      </aside>
    </>
  );
};
```

---
**12. New File: `leads_webapp1/src/pages/WabungePage.tsx`**

```typescript
// leads_webapp1/src/pages/WabungePage.tsx
import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { WabungeListItem } from '../types';
import { AdvancedSearchBar, SearchParams } from '../components/AdvancedSearchBar';
import { Pagination } from '../components/Pagination';

const API_URL = import.meta.env.VITE_API_BASE_URL;
const ITEMS_PER_PAGE = 25;

interface PaginationData {
    currentPage: number;
    totalPages: number;
    totalCount: number;
    searchAfter: string | null;
}

const SortIcon: React.FC<{ sortKey: string, currentSortBy: string, currentSortOrder: string }> = ({ sortKey, currentSortBy, currentSortOrder }) => {
    if (currentSortBy !== sortKey) return <svg className="h-4 w-4 text-slate-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l4-4 4 4m0 6l-4 4-4-4" /></svg>;
    if (currentSortOrder === 'ASC') return <svg className="h-4 w-4 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" /></svg>;
    return <svg className="h-4 w-4 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>;
};

const WabungePage: React.FC = () => {
    const [searchParams, setSearchParams] = useSearchParams();

    // --- State derived from URL ---
    const currentPage = parseInt(searchParams.get('page') || '1', 10);
    const currentSortBy = (searchParams.get('sortBy') || 'name') as 'name' | 'created_at';
    const currentSortOrder = (searchParams.get('sortOrder') || 'ASC') as 'DESC' | 'ASC';
    const currentSearchParams: SearchParams = useMemo(() => ({
        term: searchParams.get('search') || '',
        wholeWord: searchParams.get('wholeWord') === 'true',
        wholeSentence: searchParams.get('wholeSentence') === 'true',
    }), [searchParams]);

    const [items, setItems] = useState<WabungeListItem[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [paginationData, setPaginationData] = useState<PaginationData>({
        currentPage: 1,
        totalPages: 0,
        totalCount: 0,
        searchAfter: null,
    });
    const searchAfterCache = useRef<Map<number, string>>(new Map());

    useEffect(() => {
        const fetchItems = async () => {
            setLoading(true);
            setError(null);
            try {
                const params = new URLSearchParams({
                    page: String(currentPage),
                    limit: String(ITEMS_PER_PAGE),
                    sortBy: currentSortBy,
                    sortOrder: currentSortOrder,
                });
                if (currentSearchParams.term) {
                    params.append('search', currentSearchParams.term);
                    if (currentSearchParams.wholeWord) params.append('wholeWord', 'true');
                    if (currentSearchParams.wholeSentence) params.append('wholeSentence', 'true');
                }
                if (currentPage > 1) {
                    const cursor = searchAfterCache.current.get(currentPage - 1);
                    if (cursor) params.append('searchAfter', cursor);
                }

                const response = await fetch(`${API_URL}/api/wabunge?${params.toString()}`);
                if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                const data = await response.json();
                if (!data.success) throw new Error(data.message || 'Failed to fetch data.');

                setItems(data.wabunge || []);
                setPaginationData(data.pagination);
                if (data.pagination.searchAfter) {
                    searchAfterCache.current.set(currentPage, data.pagination.searchAfter);
                }
            } catch (e: any) {
                setError(e.message);
            } finally {
                setLoading(false);
            }
        };

        fetchItems();
    }, [currentPage, currentSearchParams, currentSortBy, currentSortOrder]);

    const updateParams = (newParams: Record<string, string>, resetPage = true) => {
        setSearchParams(prev => {
            Object.entries(newParams).forEach(([key, value]) => {
                if (value) {
                    prev.set(key, value);
                } else {
                    prev.delete(key);
                }
            });
            if (resetPage) prev.set('page', '1');
            return prev;
        }, { replace: true });

        if (resetPage) searchAfterCache.current.clear();
    };

    const handleSearchChange = (newSearchParams: SearchParams) => {
        updateParams({
            search: newSearchParams.term,
            wholeWord: newSearchParams.wholeWord ? 'true' : '',
            wholeSentence: newSearchParams.wholeSentence ? 'true' : ''
        });
    };

    const handleColumnSort = useCallback((column: typeof currentSortBy) => {
        const newSortOrder = currentSortBy === column && currentSortOrder === 'ASC' ? 'DESC' : 'ASC';
        updateParams({ sortBy: column, sortOrder: newSortOrder });
    }, [currentSortBy, currentSortOrder, setSearchParams]);

    const handlePageChange = (newPage: number) => {
        if (newPage > 0 && newPage <= paginationData.totalPages && newPage !== currentPage) {
            updateParams({ page: String(newPage) }, false);
            window.scrollTo(0, 0);
        }
    };
    
    const renderHighlight = (content: string | null, highlight: string[] | undefined) => {
        if (!content) return <span className="text-slate-500 dark:text-slate-400">N/A</span>;
        if (highlight && highlight.length > 0) {
            return <span dangerouslySetInnerHTML={{ __html: highlight[0] }} />;
        }
        return content;
    };

    const renderPhone = (phone: string | null) => {
        if (!phone) return <span className="text-slate-500 dark:text-slate-400">N/A</span>;
        return <a href={`tel:${phone.replace(/\s/g, '')}`} className="hover:text-primary hover:underline">{phone}</a>
    }

    const renderContent = () => {
        if (loading && items.length === 0) return <div className="text-center py-16"><h2 className="text-2xl font-semibold text-slate-900 dark:text-white">Loading...</h2></div>;
        if (error) return <div className="text-center py-16 px-4 bg-red-500/10 rounded-lg"><h2 className="text-2xl font-semibold text-red-700 dark:text-red-400">Failed to Load Data</h2><p className="text-red-600 dark:text-red-500 mt-2">{error}</p></div>;
        if (items.length > 0) {
            return (
                <>
                    <div className="bg-white dark:bg-slate-900 rounded-lg shadow-sm overflow-x-auto">
                        <table className="w-full min-w-[1200px] text-left text-sm">
                            <thead className="bg-slate-50 dark:bg-slate-800/50 text-xs uppercase text-slate-500 dark:text-slate-400">
                                <tr>
                                    <th className="p-3">
                                        <div onClick={() => handleColumnSort('name')} className="flex items-center gap-2 cursor-pointer hover:text-slate-800 dark:hover:text-white">
                                            Name
                                            <SortIcon sortKey="name" currentSortBy={currentSortBy} currentSortOrder={currentSortOrder} />
                                        </div>
                                    </th>
                                    <th className="p-3">Title</th>
                                    <th className="p-3">Phone 1</th>
                                    <th className="p-3">Phone 2</th>
                                    <th className="p-3">Chama</th>
                                    <th className="p-3">Address</th>
                                </tr>
                            </thead>
                            <tbody className="text-slate-600 dark:text-slate-300">
                                {items.map((item) => (
                                    <tr key={item.id} className="border-b border-slate-200 dark:border-slate-800 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
                                        <td className="p-3 font-semibold text-slate-800 dark:text-white">
                                            {renderHighlight(item.name, item.highlight?.name)}
                                        </td>
                                        <td className="p-3">{renderHighlight(item.title, item.highlight?.title)}</td>
                                        <td className="p-3">{renderPhone(item.phone1)}</td>
                                        <td className="p-3">{renderPhone(item.phone2)}</td>
                                        <td className="p-3">{renderHighlight(item.chama, item.highlight?.chama)}</td>
                                        <td className="p-3">{[item.address1, item.address2].filter(Boolean).join(' ')}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                    <Pagination currentPage={paginationData.currentPage} totalPages={paginationData.totalPages} onPageChange={handlePageChange} />
                </>
            );
        }

        return (
            <div className="text-center py-16 px-4 bg-white dark:bg-slate-900 rounded-lg">
                <h2 className="text-2xl font-semibold text-slate-900 dark:text-white">No Results Found</h2>
                <p className="text-slate-600 dark:text-slate-400 mt-2">{currentSearchParams.term ? `Your search for "${currentSearchParams.term}" did not match any records.` : 'There are no records to display.'}</p>
            </div>
        );
    };

    return (
        <div className="space-y-6">
            <div className="mb-8">
                <h1 className="text-4xl font-bold text-slate-900 dark:text-white mb-2">Wabunge Directory</h1>
                <p className="text-slate-600 dark:text-slate-400">
                    Browse and search through Wabunge contact data.
                    {' '}{paginationData.totalCount > 0 ? `Found ${paginationData.totalCount.toLocaleString()} records.` : ''}
                </p>
            </div>
            <AdvancedSearchBar
                placeholder="Search by name, phone, chama, or title..."
                onSearchChange={handleSearchChange}
                initialParams={{term: currentSearchParams.term, wholeWord: currentSearchParams.wholeWord, wholeSentence: currentSearchParams.wholeSentence}}
            />
            <div className={`${loading ? 'opacity-50' : ''} transition-opacity`}>
                {renderContent()}
            </div>
        </div>
    );
};

export default WabungePage;
```

---
**13. Modified File: `leads_webapp1/src/App.tsx`**

```typescript
// leads_webapp1/src/App.tsx
import React, { useState } from 'react';
import { HashRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Header } from './components/Header';
import { Sidebar } from './components/Sidebar';

import PeoplePage from './pages/PeoplePage';
import PersonDetailPage from './pages/PersonDetailPage';
import BusinessActivitiesPage from './pages/BusinessActivitiesPage';
import BusinessActivityRegistrationsPage from './pages/BusinessActivityRegistrationsPage';
import ContactDirectoryPage from './pages/ContactDirectoryPage';
import ListPage from './pages/ListPage';
import CompanyDetailPage from './pages/details/CompanyDetailPage';
import BusinessNameDetailPage from './pages/details/BusinessNameDetailPage';
import NetworkGraphPage from './pages/NetworkGraphPage/index';
import CorporateShareholdersPage from './pages/CorporateShareholdersPage';
import CorporateShareholderDetailPage from './pages/details/CorporateShareholderDetailPage';
import KeyInfluencersPage from './pages/KeyInfluencersPage';
import PhoneFilterPage from './pages/PhoneFilterPage';
import TraTinsPage from './pages/TraTinsPage';
import TRAtinsDetailPage from './pages/details/TRAtinsDetailPage';
import WabungePage from './pages/WabungePage'; // Import the new page

const App: React.FC = () => {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);

  const toggleSidebar = () => setIsSidebarOpen(prev => !prev);

  return (
    <HashRouter>
      <div className="flex flex-col min-h-screen">
        <Header onToggleSidebar={toggleSidebar} />
        <Sidebar isSidebarOpen={isSidebarOpen} toggleSidebar={toggleSidebar} />
        <main className="flex-grow container mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Routes>
            <Route path="/" element={<Navigate to="/company/dashboard" replace />} />

            {/* Company Routes */}
            <Route path="/company/dashboard" element={<ListPage registrationType="company" />} />
            <Route path="/company/people" element={<PeoplePage registrationType="company" />} />
            <Route path="/company/activities" element={<BusinessActivitiesPage registrationType="company" />} />
            <Route path="/company/activity/:activity" element={<BusinessActivityRegistrationsPage registrationType="company" />} />
            <Route path="/company/detail/:id" element={<CompanyDetailPage />} />

            {/* Business Name Routes */}
            <Route path="/business-name/dashboard" element={<ListPage registrationType="business-name" />} />
            <Route path="/business-name/people" element={<PeoplePage registrationType="business-name" />} />
            <Route path="/business-name/activities" element={<BusinessActivitiesPage registrationType="business-name" />} />
            <Route path="/business-name/activity/:activity" element={<BusinessActivityRegistrationsPage registrationType="business-name" />} />
            <Route path="/business-name/detail/:id" element={<BusinessNameDetailPage />} />
            
            {/* Corporate Shareholder Routes */}
            <Route path="/corporate-shareholders" element={<CorporateShareholdersPage />} />
            <Route path="/corporate-shareholder/:id" element={<CorporateShareholderDetailPage />} />

            {/* TRA Tins Routes */}
            <Route path="/tra-tins" element={<TraTinsPage />} />
            <Route path="/tra-tin/:id" element={<TRAtinsDetailPage />} />

            {/* Wabunge Route */}
            <Route path="/wabunge" element={<WabungePage />} />

            {/* Shared & Tool Routes */}
            <Route path="/contacts" element={<ContactDirectoryPage />} />
            <Route path="/phone-filter" element={<PhoneFilterPage />} />
            <Route path="/person/:personId" element={<PersonDetailPage />} />
            <Route path="/network-graph" element={<NetworkGraphPage />} />
            <Route path="/key-influencers" element={<KeyInfluencersPage />} />
            
            <Route path="*" element={<Navigate to="/company/dashboard" replace />} />
          </Routes>
        </main>
      </div>
    </HashRouter>
  );
};

export default App;
```
