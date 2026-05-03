package pii

import (
	"regexp"
	"strings"
)

// Catalog of PII column-name patterns. Patterns match against the lowercased,
// underscore-normalized column name. Adapted from Bearer's open-source rule set
// and Microsoft Presidio's recognizer registry.
var columnPatterns = []*regexp.Regexp{
	// identity
	regexp.MustCompile(`(^|_)(first|last|middle|maiden|given|family|sur|full|legal)_?name($|_)`),
	regexp.MustCompile(`(^|_)(displayname|display_name|nickname|username|user_name|screen_name)($|_)`),
	regexp.MustCompile(`(^|_)(initials|salutation|title|honorific)($|_)`),
	regexp.MustCompile(`(^|_)(gender|sex|pronoun|pronouns)($|_)`),
	regexp.MustCompile(`(^|_)(dob|birthdate|birth_?day|date_?of_?birth|age)($|_)`),
	regexp.MustCompile(`(^|_)(nationality|citizenship|ethnicity|race|religion)($|_)`),
	regexp.MustCompile(`(^|_)(marital_?status|civil_?status)($|_)`),

	// contact
	regexp.MustCompile(`(^|_)(email|e_?mail|mail_?address)($|_)`),
	regexp.MustCompile(`(^|_)(phone|telephone|mobile|cell|fax)(_?number|_?no)?($|_)`),
	regexp.MustCompile(`(^|_)(address|addr|street|street_?address|address_?line|locality|city|town|state|province|region|county|country|zip|zipcode|postal|postal_?code|postcode)($|_)`),
	regexp.MustCompile(`(^|_)(latitude|longitude|lat|lng|geo_?location|coords?|coordinates)($|_)`),

	// government / national IDs
	regexp.MustCompile(`(^|_)(ssn|social_?security)($|_)`),
	regexp.MustCompile(`(^|_)(nin|national_?id|national_?insurance|nhs_?number)($|_)`),
	regexp.MustCompile(`(^|_)(passport|passport_?no|passport_?number)($|_)`),
	regexp.MustCompile(`(^|_)(driver_?licen[sc]e|drivers_?licen[sc]e|dl_?number)($|_)`),
	regexp.MustCompile(`(^|_)(tax_?id|tin|vat|vat_?id|ein|siren|siret|ico)($|_)`),
	regexp.MustCompile(`(^|_)(insee|nir|rib|insurance_?number)($|_)`),

	// financial
	regexp.MustCompile(`(^|_)(credit_?card|debit_?card|card_?number|cc_?number|pan)($|_)`),
	regexp.MustCompile(`(^|_)(cvv|cvc|card_?security|card_?code)($|_)`),
	regexp.MustCompile(`(^|_)(iban|bic|swift|sort_?code|routing_?number|aba)($|_)`),
	regexp.MustCompile(`(^|_)(account_?number|bank_?account|acct_?no)($|_)`),

	// auth / secrets
	regexp.MustCompile(`(^|_)(password|passwd|pwd|passphrase)($|_)`),
	regexp.MustCompile(`(^|_)(secret|api_?key|access_?key|private_?key|client_?secret)($|_)`),
	regexp.MustCompile(`(^|_)(token|access_?token|refresh_?token|bearer|session_?id|session_?token)($|_)`),
	regexp.MustCompile(`(^|_)(otp|mfa_?code|2fa_?code|recovery_?code|backup_?code)($|_)`),
	regexp.MustCompile(`(^|_)(security_?question|security_?answer)($|_)`),

	// health
	regexp.MustCompile(`(^|_)(medical|health|diagnosis|prescription|patient|insurance_?id)($|_)`),
	regexp.MustCompile(`(^|_)(blood_?type|disability|allergy|allergies)($|_)`),

	// device / network
	regexp.MustCompile(`(^|_)(ip|ip_?address|ipv4|ipv6|mac|mac_?address|device_?id|imei|imsi|udid|advertising_?id|idfa|gaid)($|_)`),
	regexp.MustCompile(`(^|_)(user_?agent|fingerprint|device_?fingerprint)($|_)`),

	// social / online
	regexp.MustCompile(`(^|_)(facebook|twitter|linkedin|instagram|tiktok|snapchat|telegram|whatsapp|skype|github|gitlab)(_?id|_?handle|_?username|_?url)?($|_)`),
}

// Table-name hints: when the column name alone is ambiguous (e.g. "name", "id",
// "value"), being inside one of these tables raises confidence.
var sensitiveTableHints = []*regexp.Regexp{
	regexp.MustCompile(`(^|_)(user|users|customer|customers|client|clients|account|accounts|member|members|employee|employees|person|persons|people|contact|contacts|patient|patients|profile|profiles)($|_)`),
}

// Columns that are PII *only* when the surrounding table looks personal.
var ambiguousColumnPatterns = []*regexp.Regexp{
	regexp.MustCompile(`^name$`),
	regexp.MustCompile(`^fullname$`),
	regexp.MustCompile(`^value$`),
	regexp.MustCompile(`^data$`),
	regexp.MustCompile(`^content$`),
	regexp.MustCompile(`^notes?$`),
	regexp.MustCompile(`^description$`),
}

func normalize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	return s
}

// IsPII reports whether the (table, column) pair likely refers to personally
// identifiable information based on naming heuristics alone. table may be
// empty; ambiguous columns then default to non-PII.
func IsPII(table, column string) bool {
	col := normalize(column)
	if col == "" {
		return false
	}

	for _, re := range columnPatterns {
		if re.MatchString(col) {
			return true
		}
	}

	if table != "" {
		tbl := normalize(table)
		tableLooksPersonal := false
		for _, re := range sensitiveTableHints {
			if re.MatchString(tbl) {
				tableLooksPersonal = true
				break
			}
		}
		if tableLooksPersonal {
			for _, re := range ambiguousColumnPatterns {
				if re.MatchString(col) {
					return true
				}
			}
		}
	}

	return false
}
