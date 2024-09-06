import { useState } from 'react'
import './Login.scss'
import { errorHandling } from '../utils/errorHandling'
import { useNavigate } from 'react-router-dom'
import Logo from '../components/Logo'

interface LogInResponse {
  access_token?: string
  token_type?: string
  detail?: [
    {
      loc: [string, 0]
      msg: string
      type: string
    }
  ]
}

function LogIn() {
  const [formData, setFormData] = useState({
      username: '',
      password: ''
    }),
    [errorMessage, setErrorMessage] = useState('')
  const navigate = useNavigate()

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setErrorMessage('')

    try {
      console.log('formdata', formData)

      await loginUser(formData)
      navigate('/')
    } catch (error) {
      if (error instanceof Error) {
        setErrorMessage(error.message)
      }
    }
  }

  const loginUser = async ({
    username,
    password
  }: {
    username: string
    password: string
  }): Promise<void> => {
    const url = '/api/oauth2/access_token'

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: new URLSearchParams({
          username: username,
          password: password
        }),
        mode: 'cors'
      })

      const responseData: LogInResponse = await response.json()

      if (!response.ok && responseData.detail) {
        console.error('Login failed:', responseData.detail)
        setErrorMessage(responseData.detail?.[0].msg)
        return
      }

      if (responseData.access_token && responseData.token_type) {
        console.log('auth_tokenDBI', responseData.access_token)

        document.cookie = `DBI_auth_token=${responseData.access_token}; path=/; secure; samesite=strict;`
      }
    } catch (error) {
      errorHandling('POST', 'on Log in', error)
      throw new Error('Failed to log in.')
    }
  }

  return (
    <div className="login-root">
      <div className="login-container">
        <Logo />
        <div className="login-form">
          <h2>Log in</h2>
          <form onSubmit={handleSubmit}>
            <div>
              <label htmlFor="username">Username: </label>
              <input
                type="text"
                name="username"
                value={formData.username}
                onChange={(e) =>
                  setFormData({ ...formData, username: e.target.value })
                }
                required
              />
            </div>
            <div>
              <label htmlFor="password">Password: </label>
              <input
                type="password"
                name="password"
                value={formData.password}
                onChange={(e) =>
                  setFormData({ ...formData, password: e.target.value })
                }
                required
              />
            </div>
            <button
              className="login-submit-button"
              type="submit"
              disabled={!formData.username || !formData.password}
            >
              Log in
            </button>
            {errorMessage && (
              <div data-testid="error-message" className="error-message">
                {errorMessage}
              </div>
            )}
          </form>
        </div>
      </div>
    </div>
  )
}

export default LogIn
