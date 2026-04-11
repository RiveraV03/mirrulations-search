import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { motion } from "motion/react";
import { getAuthStatus } from "../api/searchApi";
import "../styles/Home.css";

import hero from "../assets/hero.jpg";

const MotionLink = motion.create(Link);

const navVariants = {
  hidden: { y: -100, opacity: 0 },
  visible: {
    y: 0,
    opacity: 1,
    transition: { type: "spring", stiffness: 260, damping: 28 },
  },
};

const heroContainerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { staggerChildren: 0.14, delayChildren: 0.15 },
  },
};

const heroItemVariants = {
  hidden: { opacity: 0, y: 28 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { type: "spring", stiffness: 220, damping: 22 },
  },
};

const sectionVariants = {
  hidden: { opacity: 0, y: 40 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.55, ease: [0.22, 1, 0.36, 1] },
  },
};

const buttonMotion = {
  rest: { scale: 1 },
  hover: { scale: 1.04, transition: { type: "spring", stiffness: 400, damping: 18 } },
  tap: { scale: 0.97 },
};

const cardReveal = {
  hidden: { opacity: 0, y: 20 },
  visible: (i) => ({
    opacity: 1,
    y: 0,
    transition: { delay: i * 0.06, duration: 0.45, ease: [0.22, 1, 0.36, 1] },
  }),
};

const GITHUB_URL = "https://github.com/Mirrulations/mirrulations-search";

export default function Home() {
  const [user, setUser] = useState(null);
  const [authLoading, setAuthLoading] = useState(true);

  useEffect(() => {
    getAuthStatus()
      .then((data) => {
        if (data.logged_in) {
          setUser({ name: data.name, email: data.email });
        }
      })
      .finally(() => setAuthLoading(false));
  }, []);

  return (
    <div className="home-page">
      <motion.header
        className="home-navbar"
        role="banner"
        variants={navVariants}
        initial="hidden"
        animate="visible"
      >
        <MotionLink to="/" className="home-nav-brand">
          Mirrulations
        </MotionLink>
        <nav className="home-nav-links" aria-label="Main navigation">
          <motion.a href="#about" className="home-nav-link" whileHover={{ y: -1 }} whileTap={{ scale: 0.98 }}>
            About
          </motion.a>
          <motion.a href="#features" className="home-nav-link" whileHover={{ y: -1 }} whileTap={{ scale: 0.98 }}>
            Features
          </motion.a>
          <motion.a
            href={GITHUB_URL}
            className="home-nav-link"
            target="_blank"
            rel="noopener noreferrer"
            whileHover={{ y: -1 }}
            whileTap={{ scale: 0.98 }}
          >
            GitHub
          </motion.a>
          <MotionLink to="/explorer" className="home-nav-link" whileHover={{ y: -1 }} whileTap={{ scale: 0.98 }}>
            Search
          </MotionLink>
          <MotionLink to="/privacy" className="home-nav-link" whileHover={{ y: -1 }} whileTap={{ scale: 0.98 }}>
            Privacy Policy
          </MotionLink>
          {!authLoading && user ? (
            <>
              <span className="home-nav-user" title={user.email}>
                {user.name}
              </span>
              <MotionLink to="/explorer" className="home-nav-cta" whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
                Explorer
              </MotionLink>
              <MotionLink to="/collections" className="home-nav-link" whileHover={{ y: -1 }} whileTap={{ scale: 0.98 }}>
                Collections
              </MotionLink>
              <motion.a href="/logout" className="home-nav-link home-nav-link--ghost" whileHover={{ y: -1 }} whileTap={{ scale: 0.98 }}>
                Sign out
              </motion.a>
            </>
          ) : (
            <motion.a href="/login" className="home-nav-google" whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
              <span className="home-nav-google-icon" aria-hidden>
                <svg viewBox="0 0 24 24" width={18} height={18}>
                  <path fill="#EA4335" d="M12 5.04c1.55 0 2.96.54 4.07 1.6l3.03-3.03C17.5 2.32 14.9 1 12 1 7.58 1 3.84 3.47 2.1 7.05l3.51 2.72A6.98 6.98 0 0 1 12 5.04z" />
                  <path fill="#4285F4" d="M22.5 12.23c0-.82-.07-1.6-.22-2.36H12v4.51h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.09-1.92 3.22-4.74 3.22-8.23z" />
                  <path fill="#FBBC05" d="M5.61 14.08A7.02 7.02 0 0 1 5.04 12c0-.72.13-1.41.35-2.08L2.1 7.05A11.95 11.95 0 0 0 1 12c0 1.93.46 3.76 1.27 5.38l3.34-2.6z" />
                  <path fill="#34A853" d="M12 23c3.24 0 5.97-1.08 7.96-2.93l-3.57-2.77c-.99.67-2.26 1.07-4.39 1.07-2.39 0-4.42-.81-5.89-2.18l-3.51 2.72C6.97 21.16 9.24 23 12 23z" />
                </svg>
              </span>
              Sign in with Google
            </motion.a>
          )}
        </nav>
      </motion.header>

      <section
        className="home-hero"
        style={{
          backgroundImage: `linear-gradient(120deg, rgba(15,23,42,0.78) 0%, rgba(30,27,75,0.58) 45%, rgba(15,23,42,0.4) 100%), url(${hero})`,
        }}
      >
        <motion.div
          className="hero-text-container"
          variants={heroContainerVariants}
          initial="hidden"
          animate="visible"
        >
          <motion.h1 className="hero-title" variants={heroItemVariants}>
            Open Regulatory Data,
            <br />
            <span className="hero-title-accent">Ready for Analysis</span>
          </motion.h1>
          <motion.p className="hero-lead" variants={heroItemVariants}>
            Mirrulations mirrors millions of U.S. federal regulatory documents and delivers them in formats built for research, transparency, and scale.
          </motion.p>
          <motion.div className="hero-buttons" variants={heroItemVariants}>
            <MotionLink
              to="/explorer"
              className="hero-btn hero-btn--primary"
              variants={buttonMotion}
              initial="rest"
              whileHover="hover"
              whileTap="tap"
            >
              Explore Dockets
            </MotionLink>
            <MotionLink
              to="/privacy"
              className="hero-btn hero-btn--outline"
              variants={buttonMotion}
              initial="rest"
              whileHover="hover"
              whileTap="tap"
            >
              Privacy Policy
            </MotionLink>
          </motion.div>
        </motion.div>
      </section>

      <motion.main
        className="home-body"
        initial="hidden"
        whileInView="visible"
        viewport={{ once: true, margin: "-60px" }}
        variants={sectionVariants}
      >
        <section id="about" className="home-section home-section--narrow">
          <motion.h2 className="home-section-heading" initial={{ opacity: 0, y: 12 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} transition={{ duration: 0.4 }}>
            Mirrulations
          </motion.h2>
          <motion.p className="home-section-lead" initial={{ opacity: 0 }} whileInView={{ opacity: 1 }} viewport={{ once: true }} transition={{ delay: 0.05, duration: 0.45 }}>
            This site is the Mirrulations Explorer: search federal dockets, organize what you follow in collections, and (when enabled) export data for offline analysis. The corpus is mirrored from regulations.gov-style sources and structured for serious use.
          </motion.p>
        </section>

        <section id="features" className="home-section home-section--wide">
          <div className="home-metrics">
            {[
              { kicker: "Scale", stat: "30M+", label: "Documents" },
              { kicker: "Platform", stat: "AWS", label: "Open Data" },
              { kicker: "Format", stat: "CSV", label: "Export Ready" },
            ].map((item, i) => (
              <motion.article key={item.label} className="home-metric-card" custom={i} initial="hidden" whileInView="visible" viewport={{ once: true, margin: "-40px" }} variants={cardReveal}>
                <span className="home-metric-kicker">{item.kicker}</span>
                <p className="home-metric-stat">{item.stat}</p>
                <p className="home-metric-label">{item.label}</p>
              </motion.article>
            ))}
          </div>

          <motion.h2 className="home-section-heading home-section-heading--center" initial={{ opacity: 0, y: 12 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} transition={{ duration: 0.4 }}>
            Why Mirrulations?
          </motion.h2>
          <div className="home-feature-grid">
            {[
              {
                title: "Massive Scale",
                body: "Nearly 30 million regulatory files mirrored directly from regulations.gov.",
              },
              {
                title: "Research Ready",
                body: "Data is structured to support text analysis without extra munging.",
              },
              {
                title: "Open & Accessible",
                body: "Hosted through AWS Open Data and available to the public.",
              },
            ].map((card, i) => (
              <motion.article key={card.title} className="home-feature-card" custom={i} initial="hidden" whileInView="visible" viewport={{ once: true, margin: "-30px" }} variants={cardReveal}>
                <h3>{card.title}</h3>
                <p>{card.body}</p>
              </motion.article>
            ))}
          </div>
        </section>

        <section id="build" className="home-section home-section--wide">
          <motion.div className="home-cta-card" initial={{ opacity: 0, y: 24 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} transition={{ duration: 0.5 }}>
            <h2>Build Research on Top of Regulation Data</h2>
            <p>Search dockets, fetch raw data, or export CSVs for your own analysis.</p>
            <div className="home-cta-row">
              <MotionLink to="/explorer" className="home-cta-primary" whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                Explore Dockets
              </MotionLink>
              <motion.a href={GITHUB_URL} className="home-cta-secondary" target="_blank" rel="noopener noreferrer" whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                View the Code
              </motion.a>
            </div>
            <p className="home-cta-footnote">
              Mirrulations is an open-source project focused on transparency, accessibility, and large-scale regulatory research.
            </p>
          </motion.div>
        </section>

        <section className="home-section home-section--wide" aria-labelledby="home-google-heading">
          <motion.div className="home-google-card" initial={{ opacity: 0, y: 16 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} transition={{ duration: 0.45 }}>
            <h2 id="home-google-heading">Sign-in &amp; data practices</h2>
            <p>
              Google sign-in uses OpenID Connect, email, and basic profile scopes so we can show your name and tie collections or downloads to your account. Use matches the{" "}
              <a href="https://developers.google.com/terms/api-services-user-data-policy" target="_blank" rel="noopener noreferrer">
                Google API Services User Data Policy
              </a>{" "}
              (including Limited Use). Read our{" "}
              <Link to="/privacy">Privacy Policy</Link> for how we collect, use, and store your information.
            </p>
          </motion.div>
        </section>

        <motion.footer className="home-page-footer" initial={{ opacity: 0 }} whileInView={{ opacity: 1 }} viewport={{ once: true }} transition={{ duration: 0.45 }}>
          <span>Mirrulations Explorer</span>
          <span className="home-page-footer-dot" aria-hidden>
            ·
          </span>
          <Link to="/privacy">Privacy Policy</Link>
          <span className="home-page-footer-dot" aria-hidden>
            ·
          </span>
          <motion.a href={GITHUB_URL} target="_blank" rel="noopener noreferrer">
            GitHub
          </motion.a>
        </motion.footer>
      </motion.main>
    </div>
  );
}
