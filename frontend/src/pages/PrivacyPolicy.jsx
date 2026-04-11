import { Link } from "react-router-dom";
import "../styles/Home.css";


export default function PrivacyPolicy() {
  return (
    <div className="legal-page">
      <header className="legal-topbar">
        <div className="legal-brand">
          <Link to="/">Mirrulations</Link>
        </div>
        <nav className="legal-nav" aria-label="Policy navigation">
          <Link className="home-btn home-btn-secondary" to="/">
            Home
          </Link>
          <a className="home-btn home-btn-primary" href="/login">
            Sign in with Google
          </a>
        </nav>
      </header>

      <main className="legal-main">
        <h1>Privacy Policy — Mirrulations Explorer</h1>
        <p className="legal-updated">Effective date: April 11, 2026</p>

        <div className="legal-note">
          This policy describes how Mirrulations Explorer handles information when you use our web application. It is
          intended to align with{" "}
          <a
            href="https://developers.google.com/terms/api-services-user-data-policy"
            rel="noopener noreferrer"
          >
            Google&apos;s API Services User Data Policy
          </a>{" "}
          and Limited Use requirements for data received from Google APIs.
        </div>

        <h2>1. Who we are</h2>
        <p>Mirrulations Explorer is a search and exploration tool for federal regulatory docket data. It is operated by students at Moravian University as part of a capstone project. For questions, contact Benjamin Coleman at <a href="mailto:colemanb@moravian.edu">colemanb@moravian.edu</a>.</p>

        <h2>2. Information we collect</h2>
        <p>
          <strong>Google sign-in.</strong> If you choose &quot;Sign in with
          Google,&quot; Google shares limited account information with us under the
          OAuth scopes configured for this application, currently including:
        </p>
        <ul>
          <li>
            <code>openid</code> — OpenID Connect identifier for your session
          </li>
          <li>
            <code>https://www.googleapis.com/auth/userinfo.email</code> — your Google
            account email address
          </li>
          <li>
            <code>https://www.googleapis.com/auth/userinfo.profile</code> — your
            display name and related basic profile fields exposed by Google for that
            scope
          </li>
        </ul>
        <p>
          We use that information only to authenticate you, display your name in the
          interface, associate your activity with your account, and operate
          features you request (such as saved collections or download jobs).
        </p>
        <p>
          <strong>Session security.</strong> After Google authenticates you, we issue
          an HTTP-only session cookie (signed token) so your browser can call our
          APIs without sending Google tokens on every request.
        </p>
        <p>
          <strong>Usage content.</strong> Search queries, filter selections, and
          results you retrieve are processed to provide the search experience. Do not
          submit sensitive personal information in free-text search unless you
          intend for it to be processed by our systems.
        </p>

        <h2>3. How we use Google user data</h2>
        <p>We use Google user data only to:</p>
        <ul>
          <li>Identify you and keep you signed in securely;</li>
          <li>Show your name (and similar profile fields supplied by Google) in the app;</li>
          <li>Associate collections, download requests, and similar features with your account;</li>
          <li>Operate, secure, debug, and improve the user-facing features of Mirrulations Explorer.</li>
        </ul>
        <p>
          We do <strong>not</strong> use Google user data for advertising, including
          personalized or retargeted ads. We do <strong>not</strong> sell Google
          user data. We do <strong>not</strong> use such data for determining
          credit-worthiness or lending. We do <strong>not</strong> allow humans to
          read your Google account data except as needed for security, abuse
          prevention, legal compliance, or with your explicit agreement for a
          specific support request.
        </p>

        <h2>4. Limited use &amp; transfers</h2>
        <p>
          Data obtained through Google APIs is handled in accordance with the Google
          API Services User Data Policy, including Limited Use restrictions. We do not
          transfer Google user data to third parties except as needed to provide or
          improve user-visible features of this app (for example, infrastructure
          hosting under appropriate agreements), for security, to comply with law,
          or as part of a merger or asset sale subject to law and, where required,
          notice and consent.
        </p>

        <h2>5. Storage and retention</h2>
        <p>
          Account identifiers (such as email and name) and data you create in the
          app (for example, collections) may be stored in our databases for as long
          as your account is active or as needed to provide the service. Download job
          metadata may be retained for a limited period consistent with operational
          needs. We may delete or anonymize data when no longer required.
        </p>

        <h2>6. Your choices</h2>
        <p>
          You can sign out and clear the session cookie using the sign-out link in
          the application. You may revoke this app&apos;s access to your Google
          account at any time from your Google Account security settings. Revoking
          access stops new sign-ins; previously stored account rows may remain until
          deleted under our retention practices.
        </p>

        

        <h2>7. Changes</h2>
        <p>
          We may update this policy to reflect product, legal, or regulatory changes.
          We will revise the effective date above when we do. Material changes may
          require additional notice under applicable law.
        </p>

        <h2>8. Contact</h2>
        <p>
          For privacy questions, please email For questions, contact Benjamin Coleman at <a href="mailto:colemanb@moravian.edu">colemanb@moravian.edu</a>.
        </p>

        <p>
          <Link to="/">Return to home</Link>
        </p>
      </main>

      <footer className="home-footer">
        <Link to="/">Home</Link>
        <span className="home-footer-sep" aria-hidden>
          ·
        </span>
        <span>Privacy Policy</span>
      </footer>
    </div>
  );
}
