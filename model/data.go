package model

//Data booking
type Data struct {
	Modification string `json:"modification"`
	ModFamily    string `json:"mod_family"`
	ModBodyType  string `json:"mod_body_type"`
	ModEngine    string `json:"mod_engine"`
	ModBase      string `json:"mod_base"`
	UrlMod       string `json:"url_mod"`
	Clientid     string `json:"clientid_google"`
	Ymuid        string `json:"ClientID"`
}
