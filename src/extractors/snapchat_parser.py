import logging
from typing import Any, Dict, List, Optional

import requests

from extractors.utils_time import utcnow_iso

logger = logging.getLogger(__name__)

class SnapchatParser:
    """
    High-level wrapper around Snapchat's public search endpoint.

    This class is intentionally resilient:
    - Handles unexpected response shapes
    - Fails gracefully with logging instead of crashing
    - Normalizes records into a stable schema
    """

    def __init__(
        self,
        base_url: str,
        timeout: int = 10,
        max_profiles: int = 100,
        proxy: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_profiles = max_profiles
        self.proxies = (
            {"http": proxy, "https": proxy} if proxy else None
        )  # Requests uses both keys
        self.session = requests.Session()
        default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
        }
        if headers:
            default_headers.update(headers)
        self.session.headers.update(default_headers)

    def search_by_keywords(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """
        Perform a search for each keyword and flatten all results
        into a single list of normalized profile dictionaries.
        """
        all_profiles: List[Dict[str, Any]] = []

        for keyword in keywords:
            logger.info("Searching Snapchat for keyword: %s", keyword)
            try:
                raw_profiles = self._search_single_keyword(keyword)
                logger.debug("Received %d raw profiles for '%s'", len(raw_profiles), keyword)
            except Exception as exc:
                logger.exception("Failed to search keyword '%s': %s", keyword, exc)
                continue

            for profile in raw_profiles:
                try:
                    normalized = self._normalize_profile(profile, keyword)
                    all_profiles.append(normalized)
                except Exception as exc:
                    logger.exception(
                        "Failed to normalize profile for keyword '%s': %s", keyword, exc
                    )

        return all_profiles

    def _search_single_keyword(self, keyword: str) -> List[Dict[str, Any]]:
        """
        Query the Snapchat search endpoint for a single keyword.

        Since Snapchat's undocumented API may change, we keep the parsing defensive:
        - Try several common container keys (profiles, results, data, etc.)
        - Fall back to list responses
        """
        params = {
            "q": keyword,
            "type": "profile",
            "count": self.max_profiles,
        }

        logger.debug("Performing HTTP GET %s with params=%s", self.base_url, params)
        response = self.session.get(
            self.base_url, params=params, timeout=self.timeout, proxies=self.proxies
        )

        if response.status_code != 200:
            logger.warning(
                "Non-200 status code from Snapchat search: %s (keyword=%s)",
                response.status_code,
                keyword,
            )
            return []

        try:
            payload = response.json()
        except ValueError:
            logger.warning("Response for keyword '%s' is not valid JSON.", keyword)
            return []

        profiles: List[Dict[str, Any]] = []

        if isinstance(payload, list):
            profiles = [p for p in payload if isinstance(p, dict)]
        elif isinstance(payload, dict):
            for key in ("profiles", "results", "accounts", "creators", "data", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    profiles = [p for p in value if isinstance(p, dict)]
                    break

        if not profiles:
            logger.info("No profiles found for keyword '%s'.", keyword)

        if self.max_profiles and len(profiles) > self.max_profiles:
            profiles = profiles[: self.max_profiles]

        return profiles

    def _normalize_profile(self, profile: Dict[str, Any], keyword: str) -> Dict[str, Any]:
        """
        Map arbitrary Snapchat API response structure into our stable schema.
        """
        # Root-level shortcuts
        location_raw = profile.get("location") or profile.get("geo") or {}
        profile_info_raw = (
            profile.get("profileInfo")
            or profile.get("profile")
            or profile.get("businessProfile")
            or {}
        )
        flags_raw = profile.get("flags") or {}

        metadata_raw = profile.get("metadata") or profile.get("meta") or {}
        business_raw = profile.get("business") or {}

        # Some Snapchat structures nest IDs differently
        id_value = (
            profile.get("id")
            or profile.get("uuid")
            or metadata_raw.get("accountId")
            or business_raw.get("accountId")
        )

        username = (
            profile.get("username")
            or profile.get("snapchatId")
            or profile_info_raw.get("username")
        )

        display_name = (
            profile.get("displayName")
            or profile.get("name")
            or profile_info_raw.get("displayName")
        )

        description = (
            profile.get("description")
            or profile.get("bio")
            or profile_info_raw.get("description")
        )

        # Subscriber count could be under several keys
        subscriber_count = (
            profile.get("subscriberCount")
            or profile.get("subscribers")
            or profile_info_raw.get("subscriberCount")
            or 0
        )

        # Verification flags
        is_verified = bool(
            profile.get("isVerified")
            or profile_info_raw.get("isVerified")
            or flags_raw.get("isVerified")
            or flags_raw.get("verified")
        )

        # Location
        location_country = (
            location_raw.get("country")
            or location_raw.get("countryName")
            or profile.get("country")
        )
        location_state = (
            location_raw.get("state")
            or location_raw.get("region")
            or location_raw.get("province")
        )
        location_display_address = (
            location_raw.get("displayAddress")
            or location_raw.get("fullAddress")
            or profile.get("address")
        )

        # Profile imagery and metadata
        logo_url = (
            profile_info_raw.get("logoUrl")
            or profile_info_raw.get("avatarUrl")
            or profile_info_raw.get("iconUrl")
        )
        hero_image_url = (
            profile_info_raw.get("heroImageUrl")
            or profile_info_raw.get("bannerUrl")
            or profile_info_raw.get("coverUrl")
        )
        created_at = (
            profile_info_raw.get("createdAt")
            or metadata_raw.get("createdAt")
            or business_raw.get("createdAt")
        )
        category = (
            profile_info_raw.get("category")
            or business_raw.get("category")
            or profile.get("category")
            or ""
        )

        # Flags
        is_lens_creator = bool(
            flags_raw.get("isLensCreator")
            or profile.get("isLensCreator")
            or business_raw.get("isLensCreator")
        )
        has_highlights = bool(
            flags_raw.get("hasHighlights")
            or profile.get("hasHighlights")
            or profile_info_raw.get("hasHighlights")
        )

        # Metadata
        organization_id = (
            metadata_raw.get("organizationId")
            or business_raw.get("organizationId")
        )

        normalized: Dict[str, Any] = {
            "id": id_value,
            "username": username,
            "displayName": display_name,
            "description": description,
            "subscriberCount": int(subscriber_count) if str(subscriber_count).isdigit() else 0,
            "isVerified": is_verified,
            "location": {
                "country": location_country,
                "state": location_state,
                "displayAddress": location_display_address,
            },
            "profileInfo": {
                "logoUrl": logo_url,
                "heroImageUrl": hero_image_url,
                "createdAt": created_at,
                "category": category,
                # Optional extra metadata if present
                "subcategory": profile_info_raw.get("subcategory", ""),
                "tier": profile_info_raw.get("tier"),
            },
            "flags": {
                "isLensCreator": is_lens_creator,
                "hasHighlights": has_highlights,
                "hasLenses": bool(flags_raw.get("hasLenses") or profile.get("hasLenses")),
                "isBrandProfile": bool(
                    flags_raw.get("isBrandProfile") or profile.get("isBrandProfile")
                ),
                "isSnapchatPlusSubscriber": bool(
                    flags_raw.get("isSnapchatPlusSubscriber")
                    or profile.get("isSnapchatPlusSubscriber")
                ),
            },
            "metadata": {
                "accountId": metadata_raw.get("accountId") or id_value,
                "organizationId": organization_id,
                "profileIconColor": metadata_raw.get("profileIconColor"),
            },
            "searchKeyword": keyword,
            "scrapedAt": utcnow_iso(),
        }

        return normalized